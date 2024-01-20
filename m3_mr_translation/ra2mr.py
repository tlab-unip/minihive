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

    def inner_mapper(self, key, value) -> tuple | None:
        return (key, value)

    def inner_reducer(self, key, values) -> tuple[str, list]:
        return key, values


"""
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
"""


def task_factory(raquery, step=1, env=ExecEnv.HDFS, optimize=False):
    assert isinstance(raquery, radb.ast.Node)

    if optimize:
        last = task_factory(raquery, step, env)
        tasks: list[RelAlgQueryTask] = [last]
        match last:
            case SelectTask() | RenameTask():
                cur_first = task_factory(raquery.inputs[0], step, env)
                while True:
                    match cur_first:
                        # Fold map only jobs
                        case SelectTask() | RenameTask():
                            tasks.insert(0, cur_first)
                        # Fold map+reduce job at the beginning
                        case JoinTask() | ProjectTask():
                            # TODO Fold a folded task
                            tasks.insert(0, cur_first)
                            break
                        case _:
                            break
                    ra = parse.one_statement_from_string(cur_first.querystring)
                    cur_first = task_factory(ra.inputs[0], step, env)
                if len(tasks) > 1:
                    return FoldedTask(
                        tasks=tasks,
                        step=step,
                        querystring=str(raquery) + ";",
                        exec_environment=env,
                    )
            case ProjectTask():
                cur_first = task_factory(raquery.inputs[0], step, env)
                while True:
                    match cur_first:
                        # Fold map only jobs
                        case SelectTask() | RenameTask():
                            tasks.insert(0, cur_first)
                        case _:
                            break
                    ra = parse.one_statement_from_string(cur_first.querystring)
                    cur_first = task_factory(ra.inputs[0], step, env)
                if len(tasks) > 1:
                    return FoldedTask(
                        tasks=tasks,
                        step=step,
                        querystring=str(raquery) + ";",
                        exec_environment=env,
                    )
            case JoinTask():
                tasks_left = []
                cur_left = task_factory(raquery.inputs[0], step, env)
                while isinstance(cur_left, SelectTask) or isinstance(
                    cur_left, RenameTask
                ):
                    tasks_left.insert(0, cur_left)
                    ra = parse.one_statement_from_string(cur_left.querystring)
                    cur_right = task_factory(ra.inputs[0], step, env)

                tasks_right = []
                cur_right = task_factory(raquery.inputs[1], step, env)
                while isinstance(cur_right, SelectTask) or isinstance(
                    cur_right, RenameTask
                ):
                    tasks_right.insert(0, cur_left)
                    ra = parse.one_statement_from_string(cur_right.querystring)
                    cur_right = task_factory(ra.inputs[0], step, env)

                # TODO nest folded task
                if len(tasks_left) > 0 and len(tasks_right) > 0:
                    return FoldedTask(
                        tasks_left=tasks_left,
                        tasks_right=tasks_right,
                        tasks=tasks,
                        step=step,
                        querystring=str(raquery) + ";",
                        exec_environment=env,
                    )

    match raquery:
        case ast.Select():
            return SelectTask(
                querystring=str(raquery) + ";",
                step=step,
                exec_environment=env,
            )
        case ast.RelRef():
            filename = raquery.rel + ".json"
            return InputData(filename=filename, exec_environment=env)
        case ast.Join():
            return JoinTask(
                querystring=str(raquery) + ";",
                step=step,
                exec_environment=env,
            )
        case ast.Project():
            return ProjectTask(
                querystring=str(raquery) + ";",
                step=step,
                exec_environment=env,
            )
        case ast.Rename():
            return RenameTask(
                querystring=str(raquery) + ";",
                step=step,
                exec_environment=env,
            )
        case _:
            raise Exception(
                "Operator " + str(type(raquery)) + " not implemented (yet)."
            )


class FoldedTask(RelAlgQueryTask):
    tasks: list[RelAlgQueryTask] = luigi.Parameter(default=[])
    # For handing join task
    tasks_left: list[RelAlgQueryTask] = luigi.Parameter(default=[])
    tasks_right: list[RelAlgQueryTask] = luigi.Parameter(default=[])
    # For handling different branch
    rels_left: set[str] = luigi.Parameter(default={})
    rels_right: set[str] = luigi.Parameter(default={})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        reducer_task = [task for task in self.tasks if task.reducer != NotImplemented]
        if len(reducer_task) != 0:
            self.reducer = self._reducer

    def requires(self):
        deps_left: list[RelAlgQueryTask] = (
            [] if len(self.tasks_left) == 0 else self.tasks_left[0].requires()
        )
        deps_right: list[RelAlgQueryTask] = (
            [] if len(self.tasks_right) == 0 else self.tasks_right[0].requires()
        )
        self.rels_left |= set(
            str(dep.filename) for dep in deps_left if isinstance(dep, InputData)
        )
        self.rels_right |= set(
            str(dep.filename) for dep in deps_right if isinstance(dep, InputData)
        )
        deps: list[RelAlgQueryTask] = deps_left + deps_right
        deps += [] if len(deps) != 0 else self.tasks[0].requires()
        return deps

    def inner_reducer(self, key, values) -> tuple[str, list]:
        # Only the first reducer encountered will be executed
        # Then the mapper of following tasks will be executed
        flag = False
        for task in self.tasks:
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
        if len(self.tasks_left) != 0 and len(self.tasks_right) != 0:
            ra_left: ast.RelExpr = parse.one_statement_from_string(
                self.tasks_left[0].querystring
            )
            # TODO Make sure deepest left is relation name
            assert len(ra_left.inputs) == 1
            assert isinstance(ra_left.inputs[0], ast.RelRef)
            self.rels_left.append(ra_left.inputs[0].rel)
            if key in self.rels_left:
                for task in self.tasks_left:
                    key, value = task.inner_mapper(key, value)
            else:
                for task in self.tasks_right:
                    key, value = task.inner_mapper(key, value)

            # yield key, value
            # return

        for task in self.tasks:
            res = task.inner_mapper(key, value)
            if res == None:
                return
            key, value = res
        yield (key, value)


class JoinTask(RelAlgQueryTask):
    """
    Mapper + Reducer
    """

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Join)

        task1 = task_factory(
            raquery.inputs[0], step=self.step + 1, env=self.exec_environment
        )
        task2 = task_factory(
            raquery.inputs[1],
            step=self.step + count_steps(raquery.inputs[0]) + 1,
            env=self.exec_environment,
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

        # for tuple1 in rels[0]:
        #     for tuple2 in rels[1]:
        #         new_tuple = dict(list(tuple1.items()) + list(tuple2.items()))
        #         return (key, json.dumps(new_tuple))
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
                raquery.inputs[0], step=self.step + 1, env=self.exec_environment
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
