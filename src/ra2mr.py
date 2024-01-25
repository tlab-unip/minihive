from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast as ast
import radb.parse as parse

"""
Control where the input data comes from, and where output data should go.
"""


class ExecEnv(Enum):
    LOCAL = 1  # read/write local files
    HDFS = 2  # read/write HDFS
    MOCK = 3  # read/write mock data to an in-memory file system.


"""
Switches between different execution environments and file systems.
"""


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


"""
Counts the number of steps / luigi tasks that we need for evaluating this query.
"""


def count_steps(raquery):
    assert isinstance(raquery, radb.ast.Node)

    if (
        isinstance(raquery, radb.ast.Select)
        or isinstance(raquery, radb.ast.Project)
        or isinstance(raquery, radb.ast.Rename)
    ):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception(
            "count_steps: Cannot handle operator " + str(type(raquery)) + "."
        )


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    """
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    """

    querystring = luigi.Parameter()

    """
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    """
    step = luigi.IntParameter(default=1)

    """
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    """

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)

    optimize = luigi.BoolParameter(default=False)

    def inner_mapper(self, key, value) -> tuple | None:
        return (key, value)

    def inner_reducer(self, key, values) -> tuple[str, list]:
        return key, values


"""
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
"""


def task_factory(raquery, step=1, env=ExecEnv.HDFS, optimize=False) -> OutputMixin:
    assert isinstance(raquery, radb.ast.Node)
    if optimize:
        task = folded_task_factory(raquery, step, env)
        if task != None:
            return task

    match raquery:
        case ast.Select():
            return SelectTask(
                querystring=str(raquery) + ";",
                step=step,
                exec_environment=env,
                optimize=optimize,
            )
        case ast.RelRef():
            filename = raquery.rel + ".json"
            return InputData(filename=filename, exec_environment=env)
        case ast.Join():
            return JoinTask(
                querystring=str(raquery) + ";",
                step=step,
                exec_environment=env,
                optimize=optimize,
            )
        case ast.Project():
            return ProjectTask(
                querystring=str(raquery) + ";",
                step=step,
                exec_environment=env,
                optimize=optimize,
            )
        case ast.Rename():
            return RenameTask(
                querystring=str(raquery) + ";",
                step=step,
                exec_environment=env,
                optimize=optimize,
            )
        case _:
            raise Exception(
                "Operator " + str(type(raquery)) + " not implemented (yet)."
            )


def folded_task_factory(raquery, step=1, env=ExecEnv.HDFS) -> OutputMixin | None:
    # From top to bottom
    last = task_factory(raquery, step, env)
    tasks: list[OutputMixin] = [last]
    match last:
        # Fold current and all previous mapper-only tasks
        # mapper-reducer task will be the first task in `tasks`
        case SelectTask() | RenameTask():
            cur_first = folded_task_factory(raquery.inputs[0], step, env)
            if cur_first == None:
                cur_first = task_factory(raquery.inputs[0], step, env)
            while True:
                match cur_first:
                    case SelectTask() | RenameTask():
                        tasks.insert(0, cur_first)
                    case FoldedTask():
                        if cur_first.reducer_tasks != None:
                            cur_first.reducer_tasks += tasks
                            cur_first.reducer = cur_first._reducer
                            return cur_first
                        else:
                            cur_first.mapper_tasks += tasks
                            return cur_first
                    case JoinTask() | ProjectTask():
                        tasks.insert(0, cur_first)
                        break
                    case _:
                        break
                ra = parse.one_statement_from_string(cur_first.querystring)
                cur_first = folded_task_factory(ra.inputs[0], step, env)
                if cur_first == None:
                    cur_first = task_factory(ra.inputs[0], step, env)
            if len(tasks) > 1:
                return FoldedTask(
                    tasks=tasks,
                    step=step,
                    querystring=str(raquery) + ";",
                    exec_environment=env,
                )
        # Fold current mapper-reducer task with all previous mapper-only tasks
        # mapper-only task will be the first task in `tasks`
        case ProjectTask():
            cur_first = folded_task_factory(raquery.inputs[0], step, env)
            if cur_first == None:
                cur_first = task_factory(raquery.inputs[0], step, env)
            while True:
                match cur_first:
                    case SelectTask() | RenameTask():
                        tasks.insert(0, cur_first)
                    case FoldedTask():
                        if cur_first.reducer_tasks == None:
                            cur_first.mapper_tasks += tasks
                            cur_first.reducer_tasks = tasks[-1:]
                            cur_first.reducer = cur_first._reducer
                            return cur_first
                        else:
                            break
                    case _:
                        break
                ra = parse.one_statement_from_string(cur_first.querystring)
                cur_first = folded_task_factory(ra.inputs[0], step, env)
                if cur_first == None:
                    cur_first = task_factory(ra.inputs[0], step, env)
            if len(tasks) > 1:
                return FoldedTask(
                    tasks=tasks,
                    step=step,
                    querystring=str(raquery) + ";",
                    exec_environment=env,
                )
        # Fold current mapper-reducer task with all previous mapper-only tasks
        # mapper-only task will be the first task in `left_tasks` and `right_tasks`
        case JoinTask():
            tasks_left: list[OutputMixin] = []
            cur_left = folded_task_factory(raquery.inputs[0], step, env)
            if cur_left == None:
                cur_left = task_factory(raquery.inputs[0], step, env)
            while True:
                match cur_left:
                    case SelectTask() | RenameTask():
                        tasks_left.insert(0, cur_left)
                    case FoldedTask():
                        if cur_left.reducer_tasks == None:
                            cur_left.mapper_tasks += tasks_left
                            cur_left.reducer_tasks = tasks_left[-1:]
                            cur_left.reducer = cur_left._reducer
                            return cur_left
                        else:
                            break
                    case _:
                        break
                ra = parse.one_statement_from_string(cur_left.querystring)
                cur_left = folded_task_factory(ra.inputs[0], step, env)
                if cur_left == None:
                    cur_left = task_factory(ra.inputs[0], step, env)

            tasks_right: list[OutputMixin] = []
            cur_right = folded_task_factory(raquery.inputs[1], step, env)
            if cur_right == None:
                cur_right = task_factory(raquery.inputs[1], step, env)
            while True:
                match cur_right:
                    case SelectTask() | RenameTask():
                        tasks_right.insert(0, cur_right)
                    case FoldedTask():
                        if cur_right.reducer_tasks == None:
                            cur_right.mapper_tasks += tasks_right
                            cur_right.reducer_tasks = tasks_right[-1:]
                            cur_right.reducer = cur_right._reducer
                            return cur_right
                        else:
                            break
                    case _:
                        break
                ra = parse.one_statement_from_string(cur_right.querystring)
                cur_right = folded_task_factory(ra.inputs[0], step, env)
                if cur_right == None:
                    cur_right = task_factory(ra.inputs[0], step, env)

            # TODO fix deps
            if len(tasks_left) > 0 or len(tasks_right) > 0:
                # TODO edge case, same required relations
                if len(tasks_left) > 0 and len(tasks_right) > 0:
                    req_left = set(
                        str(req.filename).split(".")[0]
                        for req in tasks_left[0].requires()
                        if isinstance(req, InputData)
                    )
                    req_right = set(
                        str(req.filename).split(".")[0]
                        for req in tasks_right[0].requires()
                        if isinstance(req, InputData)
                    )
                    if len(req_left.intersection(req_right)) != 0:
                        return None

                return FoldedTask(
                    tasks_left=tasks_left,
                    tasks_right=tasks_right,
                    tasks=tasks,
                    step=step,
                    querystring=str(raquery) + ";",
                    exec_environment=env,
                )
    return None


class FoldedTask(RelAlgQueryTask):
    tasks: list[RelAlgQueryTask] = luigi.Parameter(default=[])
    # For handing join task (only at the mapper stage)
    tasks_left: list[RelAlgQueryTask] = luigi.Parameter(default=[])
    tasks_right: list[RelAlgQueryTask] = luigi.Parameter(default=[])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # For handling different branches (only at the mapper stage)
        # Default by relation name
        self.keys_left = set[str]()
        self.keys_right = set[str]()
        self.tasks = list(self.tasks)

        indices = [
            i for i, task in enumerate(self.tasks) if task.reducer != NotImplemented
        ]
        self.mapper_tasks: list[RelAlgQueryTask] = self.tasks
        self.reducer_tasks = None
        if len(indices) != 0:
            assert len(indices) == 1
            self.reducer = self._reducer
            self.mapper_tasks = self.tasks[: indices[0] + 1]
            self.reducer_tasks = self.tasks[indices[0] :]

    def requires(self):
        # Dependent tasks from deepest task after conditional branches
        deps_main = self.tasks[0].requires()
        # Dependent tasks from deepest task in conditional branches
        deps_left: list[OutputMixin] = []
        deps_right: list[OutputMixin] = []

        # Find relation name in dependent tasks
        if len(self.tasks_left) != 0:
            deps_left += self.tasks_left[0].requires()
        elif len(deps_main) == 2:
            deps_left.append(deps_main[0])
        for dep in deps_left:
            match dep:
                case InputData():
                    self.keys_left.add(str(dep.filename).split(".")[0])
                case RenameTask():
                    ra: ast.Rename = parse.one_statement_from_string(dep.querystring)
                    self.keys_left.add(ra.relname)

        if len(self.tasks_right) != 0:
            deps_right += self.tasks_right[0].requires()
        elif len(deps_main) == 2:
            deps_right.append(deps_main[1])
        for dep in deps_right:
            match dep:
                case InputData():
                    self.keys_right.add(str(dep.filename).split(".")[0])
                case RenameTask():
                    ra: ast.Rename = parse.one_statement_from_string(dep.querystring)
                    self.keys_right.add(ra.relname)

        deps: list[OutputMixin] = deps_left + deps_right
        deps += [] if len(deps) != 0 else self.tasks[0].requires()
        return deps

    def inner_mapper(self, key, value) -> tuple | None:
        """
        if key is in left deps, execute all mappers in left tasks.
        else if key in right deps, execute all mappers in right tasks.
        finally, execute all mappers before the reducer task
        """
        if len(self.tasks_left) != 0 or len(self.tasks_right) != 0:
            # TODO fix cases
            match key:
                case key if key in self.keys_left:
                    for task in self.tasks_left:
                        res = task.inner_mapper(key, value)
                        if res == None:
                            return
                        key, value = res
                case key if key in self.keys_right:
                    for task in self.tasks_right:
                        res = task.inner_mapper(key, value)
                        if res == None:
                            return
                        key, value = res

        for task in self.mapper_tasks:
            res = task.inner_mapper(key, value)
            if res == None:
                return
            key, value = res
        return key, value

    def inner_reducer(self, key, values) -> tuple[str, list]:
        """
        Execute the first and only reducer in tasks
        and any mapper in following tasks
        """
        flag = False
        for task in self.reducer_tasks:
            if flag:
                new_values = []
                for value in values:
                    res = task.inner_mapper(key, value)
                    if res != None:
                        _, value = res
                        new_values.append(value)
                values = new_values
            if not flag and task.reducer != NotImplemented:
                flag = True
                key, values = task.inner_reducer(key, values)
        return key, values

    def _reducer(self, key, values):
        key, values = self.inner_reducer(key, values)
        for value in values:
            yield key, value

    def mapper(self, line):
        key, value = line.split("\t")
        result = self.inner_mapper(key, value)
        if result != None:
            yield result


class JoinTask(RelAlgQueryTask):
    """
    Mapper + Reducer
    """

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Join)

        task1 = task_factory(
            raquery.inputs[0],
            step=self.step + 1,
            env=self.exec_environment,
            optimize=self.optimize,
        )
        task2 = task_factory(
            raquery.inputs[1],
            step=self.step + count_steps(raquery.inputs[0]) + 1,
            env=self.exec_environment,
            optimize=self.optimize,
        )

        return [task1, task2]

    def inner_mapper(self, key, value):
        json_tuple: dict = json.loads(value)

        condition: ast.ValExprBinaryOp = radb.parse.one_statement_from_string(
            self.querystring
        ).cond

        """ ...................... fill in your code below ........................"""

        conds: list[ast.ValExprBinaryOp] = []
        queue = [condition]
        while len(queue) > 0:
            top = queue.pop(0)
            if isinstance(top, ast.ValExprBinaryOp):
                if top.op != parse.RAParser.AND:
                    conds.append(top)
                    continue
            queue += top.inputs

        # Assume `cond` is (attribute, value) pair
        attrs: str = []
        for cond in conds:
            # Filter attributes that exists in tuple
            for input in cond.inputs:
                assert isinstance(input, ast.AttrRef)
                if input.rel != None and str(input) in json_tuple.keys():
                    attrs.append(str(input))
                elif input.rel == None:
                    attrs += [
                        key
                        for key in json_tuple.keys()
                        if key.split(".")[1] == input.name
                    ]
        # Value of the attribute, will be used as key
        rel_vals = [
            json_tuple.get(attr) for attr in attrs if json_tuple.get(attr) != None
        ]
        # Assume op is AND
        match cond.op:
            case parse.RAParser.EQ:
                return (rel_vals, value)
        """ ...................... fill in your code above ........................"""

    def inner_reducer(self, key, values):
        # raquery = radb.parse.one_statement_from_string(self.querystring)
        """...................... fill in your code below ........................"""
        # for value in values:
        #     return (key, value)
        # return

        rels: list[list] = [[], []]
        prev_keys: set[str] = {}
        index = 1
        for value in values:
            json_tuple = json.loads(value)
            cur_keys = set(json_tuple.keys())
            if len(cur_keys.difference(prev_keys)) != 0:
                prev_keys = cur_keys
                index = (index + 1) % 2
                rels[index].append(json_tuple)
            else:
                rels[index].append(json_tuple)

        assert len(rels) == 2
        values = [
            json.dumps(dict(list(tuple1.items()) + list(tuple2.items())))
            for tuple1 in rels[0]
            for tuple2 in rels[1]
        ]
        return key, values
        """ ...................... fill in your code above ........................"""

    def mapper(self, line):
        key, value = line.split("\t")
        result = self.inner_mapper(key, value)
        if result != None:
            yield result

    def reducer(self, key, values):
        key, values = self.inner_reducer(key, values)
        for value in values:
            yield key, value


class SelectTask(RelAlgQueryTask):
    """
    Mapper Only
    """

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Select)

        return [
            task_factory(
                raquery.inputs[0], step=self.step + 1, env=self.exec_environment
            )
        ]

    def inner_mapper(self, key, value):
        json_tuple: dict = json.loads(value)

        condition: ast.ValExprBinaryOp = radb.parse.one_statement_from_string(
            self.querystring
        ).cond

        """ ...................... fill in your code below ........................"""
        conds: list[ast.ValExprBinaryOp] = []
        queue = [condition]
        while len(queue) > 0:
            top = queue.pop(0)
            if isinstance(top, ast.ValExprBinaryOp):
                if top.op != parse.RAParser.AND:
                    conds.append(top)
                    continue
            queue += top.inputs

        # TODO Assume op is AND, cond is (attribute, value) pair
        for cond in conds:
            attr: ast.AttrRef = [
                input for input in cond.inputs if isinstance(input, ast.AttrRef)
            ][0]
            val: str = [
                input
                for input in cond.inputs
                if isinstance(input, ast.RANumber) or isinstance(input, ast.RAString)
            ][0].val.strip("'")

            rel_attr = (
                str(attr)
                if attr.rel != None
                else [
                    key for key in json_tuple.keys() if key.split(".")[1] == attr.name
                ][0]
            )

            rel_val = json_tuple.get(rel_attr)
            match cond.op:
                case parse.RAParser.EQ:
                    if val != str(rel_val):
                        # yield (relation, val + "," + str(rel_val))
                        return
        return (key, value)
        """ ...................... fill in your code above ........................"""

    def mapper(self, line: str):
        relation, tuple = line.split("\t")
        res = self.inner_mapper(relation, tuple)
        if res != None:
            yield res


class RenameTask(RelAlgQueryTask):
    """
    Mapper Only
    """

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Rename)

        return [
            task_factory(
                raquery.inputs[0], step=self.step + 1, env=self.exec_environment
            )
        ]

    def inner_mapper(self, key, value) -> tuple | None:
        json_tuple = json.loads(value)

        raquery: ast.Rename = radb.parse.one_statement_from_string(self.querystring)

        """ ...................... fill in your code below ........................"""
        # TODO ignore attributes
        new_rel = raquery.relname
        new_tuple = dict(
            [
                (new_rel + "." + item[0].split(".")[1], item[1])
                for item in json_tuple.items()
            ]
        )
        return (new_rel, json.dumps(new_tuple))
        """ ...................... fill in your code above ........................"""

    def mapper(self, line):
        relation, tuple = line.split("\t")
        yield self.inner_mapper(relation, tuple)


class ProjectTask(RelAlgQueryTask):
    """
    Mapper + Reducer
    """

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Project)

        return [
            task_factory(
                raquery.inputs[0],
                step=self.step + 1,
                env=self.exec_environment,
                optimize=self.optimize,
            )
        ]

    def inner_mapper(self, key, value) -> tuple | None:
        json_tuple = json.loads(value)

        attrs: list[ast.AttrRef] = radb.parse.one_statement_from_string(
            self.querystring
        ).attrs
        """ ...................... fill in your code below ........................"""
        tuple_list = []
        for attr in attrs:
            if attr.rel != None:
                rel_attr = str(attr)
            else:
                rel_attr = [
                    key for key in json_tuple.keys() if key.split(".")[1] == attr.name
                ][0]
            tuple_list.append((rel_attr, json_tuple.get(rel_attr)))
        result = json.dumps(dict(tuple_list))
        return (0, result)
        """ ...................... fill in your code above ........................"""

    def inner_reducer(self, key, values):
        """...................... fill in your code below ........................"""
        return key, list(set(values))
        """ ...................... fill in your code above ........................"""

    def mapper(self, line):
        relation, tuple = line.split("\t")
        yield self.inner_mapper(relation, tuple)

    def reducer(self, key, values):
        key, values = self.inner_reducer(key, values)
        for value in values:
            yield key, value


if __name__ == "__main__":
    import luigi.mock as mock
    from test_ra2mr import prepareMockFileSystem

    prepareMockFileSystem()

    querystring = (
        "\project_{A.name, B.name} "
        "((\\rename_{A: *} Eats) \join_{A.pizza = B.pizza} (\\rename_{B: *} Eats));"
    )

    raquery = radb.parse.one_statement_from_string(querystring)
    task = task_factory(raquery, env=ExecEnv.MOCK)
    luigi.build([task], local_scheduler=True)

    data_list = mock.MockFileSystem().listdir("")
    print(data_list)

    f = lambda _task: [
        f(_dep) or print(_dep) for _dep in _task.deps() if isinstance(_dep, luigi.Task)
    ]
    f(task)

    _output = []
    with task.output().open("r") as f:
        for line in f:
            _output.append(line)
    print("".join("{}: {}".format(*k) for k in enumerate(_output)))
    print(len(_output))
