import sqlparse
import sqlparse.tokens as tokens
import sqlparse.sql as sql
import radb
import radb.parse
import radb.ast as ast


def translate(stmt: sql.Statement) -> ast.RelExpr:
    # Seperate clauses by keyword
    tkn_list: list[sql.Token] = [x for x in stmt.flatten() if not x.is_whitespace]
    clauses: dict[str, list[sql.Token]] = {"select": [], "from": [], "where": []}
    cur_clause = None
    for tkn in tkn_list:
        if tkn.ttype in tokens.DML:
            cur_clause = clauses["select"]
        elif tkn.ttype in tokens.Keyword:
            match tkn.value.upper():
                case "FROM":
                    cur_clause = clauses["from"]
                case "WHERE":
                    cur_clause = clauses["where"]
                case _:
                    cur_clause.append(tkn)
        else:
            cur_clause.append(tkn)

    input = from_parser(clause=clauses["from"])
    input = where_parser(clause=clauses["where"], input=input)
    input = select_parser(clause=clauses["select"], input=input)

    return input


def from_parser(clause: list[tokens.Token]) -> ast.RelExpr:
    """
    Parse `From` Clause
    1. Check alias, `AS` is omitted
    2. Get Relations
    """
    rels: list[ast.RelRef] = []
    temp: list[tokens.Token] = []
    for tkn in clause:
        if not tkn.ttype in tokens.Punctuation:
            temp.append(tkn)
        else:
            if len(temp) == 2:
                rels.append(
                    ast.Rename(
                        relname=temp[1].value,
                        attrnames=None,
                        input=ast.RelRef(rel=temp[0].value),
                    )
                )
            elif len(temp) == 1:
                rels.append(ast.RelRef(rel=temp[0].value))
            temp.clear()
    if len(temp) == 2:
        rels.append(
            ast.Rename(
                relname=temp[1].value,
                attrnames=None,
                input=ast.RelRef(rel=temp[0].value),
            )
        )
    elif len(temp) == 1:
        rels.append(ast.RelRef(rel=temp[0].value))
    temp.clear()

    input = rels[0]
    for i in range(len(rels) - 1):
        input = ast.Cross(input, rels[i + 1])

    return input


def where_parser(clause: list[tokens.Token], input: ast.RelExpr) -> ast.RelExpr:
    """
    Parse `Where` Clause
    """
    # Pasing `a` or `R.a`
    temp_tkns: list[tokens.Token] = []
    # Parsing `a = b`
    temp_attrs: list[ast.AttrRef | ast.RAString | ast.RANumber | int] = []
    # Parsing `(a = b) and (c = d)`
    temp_attrs_2: list[ast.ValExprBinaryOp | int] = []
    for tkn in clause:
        # Assembling tokens into attribute if there's any
        if tkn.ttype in tokens.Keyword or tkn.ttype in tokens.Operator:
            # Parsing `a` or `R.a` from `temp_tkns`
            match len(temp_tkns):
                case 1:
                    temp_attrs.append(ast.AttrRef(rel=None, name=temp_tkns[0].value))
                case 3:
                    temp_attrs.append(
                        ast.AttrRef(rel=temp_tkns[0].value, name=temp_tkns[2].value)
                    )
            temp_tkns.clear()

        # Collecting token inside `a = b`
        if not tkn.ttype in tokens.Keyword:
            # Collecting token as operand or into `temp_tkns`
            if not tkn.ttype in tokens.Operator:
                if tkn.ttype in tokens.String:
                    temp_attrs.append(ast.RAString(tkn.value))
                elif tkn.ttype in tokens.Number:
                    temp_attrs.append(ast.RANumber(tkn.value))
                # Generate relation attribute
                else:
                    temp_tkns.append(tkn)

            else:
                op = None
                match tkn.value:
                    case "=":
                        op = ast.sym.EQ
                    case "<":
                        op = ast.sym.LT
                    case ">":
                        op = ast.sym.GT
                    case "<=":
                        op = ast.sym.LE
                    case ">=":
                        op = ast.sym.GE
                assert op != None
                temp_attrs.append(op)
        # Assembling attributes from `temp_attrs` into a `BinaryOp`
        else:
            op = None
            match tkn.value.upper():
                case "AND":
                    op = ast.sym.AND

            assert len(temp_attrs) == 3 and op != None
            temp_attrs_2.append(
                ast.ValExprBinaryOp(temp_attrs[0], temp_attrs[1], temp_attrs[2])
            )
            temp_attrs_2.append(op)
            temp_attrs.clear()

    # When end of clause
    match len(temp_tkns):
        case 1:
            temp_attrs.append(ast.AttrRef(rel=None, name=temp_tkns[0].value))
        case 3:
            temp_attrs.append(
                ast.AttrRef(rel=temp_tkns[0].value, name=temp_tkns[2].value)
            )
    temp_tkns.clear()

    if len(temp_attrs) == 3:
        temp_attrs_2.append(
            ast.ValExprBinaryOp(temp_attrs[0], temp_attrs[1], temp_attrs[2])
        )
        temp_attrs.clear()

    if len(temp_attrs_2) != 0:
        cond = temp_attrs_2[0]
        if len(temp_attrs_2) >= 3:
            for i in range(0, len(temp_attrs_2) - 2, 2):
                cond = ast.ValExprBinaryOp(
                    cond, temp_attrs_2[i + 1], temp_attrs_2[i + 2]
                )
        input = ast.Select(cond=cond, input=input)

    return input


def select_parser(clause: list[tokens.Token], input: ast.RelExpr) -> ast.RelExpr:
    """
    Parse `Select` Clause
    """
    attrs: list[ast.AttrRef] = []
    temp: list[tokens.Token] = []
    for tkn in clause:
        if tkn.ttype in tokens.Keyword:
            match tkn.value.upper():
                case "DISTINCT":
                    pass
            continue
        elif tkn.ttype in tokens.Wildcard:
            break
        elif tkn.ttype in tokens.Punctuation and tkn.value == ",":
            match len(temp):
                case 1:
                    attrs.append(ast.AttrRef(rel=None, name=temp[0].value))
                case 3:
                    attrs.append(ast.AttrRef(rel=temp[0].value, name=temp[2].value))
            temp.clear()
        else:
            temp.append(tkn)
    match len(temp):
        case 1:
            attrs.append(ast.AttrRef(rel=None, name=temp[0].value))
        case 3:
            attrs.append(ast.AttrRef(rel=temp[0].value, name=temp[2].value))
    temp.clear()

    if len(attrs) != 0:
        input = ast.Project(attrs=attrs, input=input)

    return input


if __name__ == "__main__":
    statements = [
        (
            "select distinct Person.name, Serves.pizzeria from Person, Eats, Serves "
            "where Person.name = Eats.name and Eats.pizza = Serves.pizza "
            "and Eats.pizza = 'mushroom'"
        ),
        (
            "select distinct P.name, S.pizzeria from Person P, Eats E, Serves S "
            "where P.name = E.name and E.pizza = S.pizza "
            "and E.pizza = 'mushroom'"
        ),
        (
            "select distinct P1.name "
            "from Person P1, Eats Eats1, Person P2, Eats Eats2 where P1.name = Eats1.name and P2.name = Eats2.name "
            "and P1.name = P2.name and P1.age = 16 "
        ),
        (
            "select distinct Serves.pizzeria from Person, Eats, Serves "
            "where Person.name = Eats.name and Eats.pizza = Serves.pizza "
            "and Eats.pizza = 'mushroom' and Serves.price = 11"
        ),
        (
            "select distinct * from Person, Eats, Serves "
            "where Person.name = Eats.name and Eats.pizza = Serves.pizza "
            "and Person.age = 16 and Serves.pizzeria = 'Little Ceasars'"
        ),
        ("select distinct A.name, B.name from Eats A, Eats B where A.pizza = B.pizza"),
    ]
    for stmt in statements:
        ra = translate(sqlparse.parse(stmt)[0])
        print(ra)
