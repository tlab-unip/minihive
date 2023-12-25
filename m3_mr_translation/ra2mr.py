from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast as ast
import radb.parse as parse
from types import SimpleNamespace

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


"""
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
"""


def task_factory(raquery, step=1, env=ExecEnv.HDFS):
    assert isinstance(raquery, radb.ast.Node)

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(
            querystring=str(raquery) + ";", step=step, exec_environment=env
        )

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(
            querystring=str(raquery) + ";", step=step, exec_environment=env
        )

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(
            querystring=str(raquery) + ";", step=step, exec_environment=env
        )

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")


class JoinTask(RelAlgQueryTask):
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

    def mapper(self, line):
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)

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
        attrs: ast.AttrRef = []
        for cond in conds:
            attrs += [
                input
                for input in cond.inputs
                if isinstance(input, ast.AttrRef) and input.rel == relation
            ]

        rel_vals = [
            json_tuple.get(relation + "." + attr.name)
            for attr in attrs
            if json_tuple.get(relation + "." + attr.name) != None
        ]
        match cond.op:
            case parse.RAParser.EQ:
                yield (rel_vals, line)
        """ ...................... fill in your code above ........................"""

    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        """ ...................... fill in your code below ........................"""

        rels: dict[str, list] = {}
        for value in values:
            relation, tuple = value.split("\t")
            json_tuple = json.loads(tuple)
            if rels.get(relation) == None:
                rels[relation] = []
            rels[relation].append(json_tuple)

        if len(rels) == 2:
            rel1, rel2 = rels
            for tuple1 in rels[rel1]:
                for tuple2 in rels[rel2]:
                    new_tuple = dict(
                        list(tuple1.items())
                        + list(tuple2.items())
                        # + [(attr, val)]
                    )
                    yield (key, json.dumps(new_tuple))
        """ ...................... fill in your code above ........................"""


class SelectTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Select)

        return [
            task_factory(
                raquery.inputs[0], step=self.step + 1, env=self.exec_environment
            )
        ]

    def mapper(self, line: str):
        relation, tuple = line.split("\t")
        json_tuple: dict = json.loads(tuple)

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

            rel_val = json_tuple.get(relation + "." + attr.name)
            match cond.op:
                case parse.RAParser.EQ:
                    if val != str(rel_val):
                        # yield (relation, val + "," + str(rel_val))
                        return
        yield (relation, tuple)
        """ ...................... fill in your code above ........................"""


class RenameTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Rename)

        return [
            task_factory(
                raquery.inputs[0], step=self.step + 1, env=self.exec_environment
            )
        ]

    def mapper(self, line):
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)

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
        yield (new_rel, json.dumps(new_tuple))
        """ ...................... fill in your code above ........................"""


class ProjectTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert isinstance(raquery, radb.ast.Project)

        return [
            task_factory(
                raquery.inputs[0], step=self.step + 1, env=self.exec_environment
            )
        ]

    def mapper(self, line):
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)

        attrs: list[ast.AttrRef] = radb.parse.one_statement_from_string(
            self.querystring
        ).attrs
        """ ...................... fill in your code below ........................"""
        new_tuple = dict(
            [
                (attr.rel + "." + attr.rel, json_tuple.get(attr.rel + "." + attr.name))
                for attr in attrs
                if json_tuple.get(attr.rel + "." + attr.name) != None
            ]
        )
        yield (relation, json.dumps(new_tuple))
        """ ...................... fill in your code above ........................"""

    def reducer(self, key, values):
        """...................... fill in your code below ........................"""
        for value in values:
            yield (key, value)
        """ ...................... fill in your code above ........................"""


if __name__ == "__main__":
    from test_ra2mr import prepareMockFileSystem
    import luigi.mock as mock

    prepareMockFileSystem()
    # querystring = "\select_{gender='female' and age=16}(Person);"
    # querystring = "\select_{P.gender='female'} \\rename_{P:*} (Person);"
    # querystring = (
    #     "Person \join_{Person.name = Eats.name} (\select_{pizza='mushroom'} Eats);"
    # )
    # querystring = "\project_{pizza} \select_{pizza='mushroom'} Eats;"
    # querystring = "(\\rename_{P:*} Person) \join_{P.gender = Q.gender and P.age = Q.age} (\\rename_{Q:*} Person);"
    querystring = (
        "(Person \join_{Person.name = Eats.name} Eats) "  # "\join_{Eats.pizza = Serves.pizza} (\select_{pizzeria='Dominos'} Serves)
        ";"
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
    print(_output)
    print(len(_output))
