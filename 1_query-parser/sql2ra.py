import sqlparse
import sqlparse.tokens as tokens
import sqlparse.sql as sql
import radb
import radb.ast as ast


def translate(stmt: sql.Statement) -> ast.Project:
    # Seperate clauses by keyword
    tkn_list: list[sql.Token] = [x for x in stmt.flatten() if not x.is_whitespace]
    clauses: dict[str, list[sql.Token]] = {"select": [], "from": [], "where": []}
    cur_clause = None
    for tkn in tkn_list:
        if tkn.ttype in tokens.DML:
            cur_clause = clauses["select"]
        elif tkn.ttype in tokens.Keyword:
            if tkn.value.upper() == "FROM":
                cur_clause = clauses["from"]
            elif tkn.value.upper() == "WHERE":
                cur_clause = clauses["where"]
            else:
                cur_clause.append(tkn)
        else:
            cur_clause.append(tkn)
    # print(clauses)

    """
    Parse `From` Clause
    1. Check alias, `AS` is omitted
    Alias: Original Name
    2. Get Relations
    """
    alias: dict[tokens.Token, tokens.Token] = {}
    temp: list[tokens.Token] = []
    for tkn in clauses["from"]:
        if not tkn.ttype in tokens.Punctuation:
            temp.append(tkn)
        else:
            if len(temp) == 2:
                alias[temp[1]] = temp[0]
            elif len(temp) == 1:
                alias[temp[0]] = None
            temp.clear()
    if len(temp) == 2:
        alias[temp[1]] = temp[0]
    elif len(temp) == 1:
        alias[temp[0]] = None
    temp.clear()

    # print(alias)
    input = ast.RelExpr(inputs=[ast.RelRef(x.value) for x in alias.keys()])

    """
    Parse `Where` Clause
    Clause -> Condition -> Attribute
    """
    # print(clauses["where"])
    temp_tkns: list[tokens.Token] = []
    temp_attrs: list[ast.AttrRef | ast.RAString | ast.RANumber | int] = []
    temp_conds: list[ast.ValExpr] = []
    for tkn in clauses["where"]:
        if tkn.ttype in tokens.Keyword or tkn.ttype in tokens.Operator:
            match len(temp_tkns):
                case 1:
                    temp_attrs.append(ast.AttrRef(rel=None, name=temp_tkns[0].value))
                case 3:
                    temp_attrs.append(
                        ast.AttrRef(rel=temp_tkns[0].value, name=temp_tkns[2].value)
                    )
            temp_tkns.clear()

        if not tkn.ttype in tokens.Keyword:
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
                temp_attrs.append(op)

        elif tkn.value.upper() == "AND":
            if len(temp_attrs) == 3:
                temp_conds.append(
                    ast.ValExprBinaryOp(temp_attrs[0], temp_attrs[1], temp_attrs[2])
                )
                temp_attrs.clear()
            else:
                print("Condition Failed", temp_attrs)

    match len(temp_tkns):
        case 1:
            temp_attrs.append(ast.AttrRef(rel=None, name=temp_tkns[0].value))
        case 3:
            temp_attrs.append(
                ast.AttrRef(rel=temp_tkns[0].value, name=temp_tkns[2].value)
            )
    temp_tkns.clear()
    if len(temp_attrs) == 3:
        temp_conds.append(
            ast.ValExprBinaryOp(temp_attrs[0], temp_attrs[1], temp_attrs[2])
        )
        temp_attrs.clear()

    if len(temp_conds) != 0:
        cond = ast.ValExpr(inputs=temp_conds)
        input = ast.Select(cond=cond, input=input)
    # print(input.to_json())

    """
    Parse `Select` Clause
    """
    # print(clauses["select"])
    attrs: list[ast.AttrRef] = []
    temp: list[tokens.Token] = []
    for tkn in clauses["select"]:
        if tkn.ttype in tokens.Keyword:
            match tkn.value.upper():
                case "DISTINCT":
                    pass
            continue
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


def test_select_node():
    condition = ast.ValExprBinaryOp(
        ast.AttrRef(rel="person", name="age"), ast.sym.LE, ast.RANumber("10")
    )
    relation = ast.RelRef("person")
    return ast.Select(cond=condition, input=relation)


if __name__ == "__main__":
    sqlstmt = "select distinct name from person where gender='female'"
    stmt = sqlparse.parse(sqlstmt)[0]

    sqlstmt2 = "select distinct T1.a, T2.b from Test1 T1, Test2 T2 where T1.foo = T2.bar and 'foo' = T2.bar"
    stmt2 = sqlparse.parse(sqlstmt2)[0]

    ra = translate(stmt2)
    print(ra.to_json())
