import radb
import radb.parse as parse
import radb.ast as ast


def rule_break_up_selections(ra: ast.RelExpr) -> ast.RelExpr:
    # child, parent
    parent_record: dict[ast.RelExpr, ast.RelExpr] = {ra: None}
    while len(parent_record) > 0:
        node = list(parent_record.keys())[0]
        parent = parent_record.pop(node)

        for input in node.inputs:
            parent_record[input] = node
        # Find a selection
        if type(node) == ast.Select:
            origin = node
            cond = node.cond

            # Break up the condition
            while cond.op == parse.RAParser.AND:
                left, right = cond.inputs
                input = ast.Select(cond=right, input=node.inputs[0])
                node = ast.Select(cond=left, input=input)
                cond = node.cond

            # Replace input node in parent
            if parent != None:
                for i, _input in enumerate(parent.inputs):
                    if origin == parent.inputs[i]:
                        parent.inputs[i] = node
            else:
                ra = node

    return ra


def rule_push_down_selections(
    ra: ast.RelExpr, dd: dict[str, dict[str, str]]
) -> ast.RelExpr:
    """
    Assumes that conjunctions in selections have been broken up.
    """
    # child, parent
    parent_record: dict[ast.RelExpr, ast.RelExpr] = {ra: None}
    while len(parent_record) > 0:
        node = list(parent_record.keys())[0]
        parent = parent_record.pop(node)

        for input in node.inputs:
            parent_record[input] = node
        # Find a selection
        if type(node) == ast.Select:
            origin = node
            cond = node.cond
            attrs_cond: list[ast.AttrRef | ast.RAString | ast.RANumber] = cond.inputs
            # print([str(x) for x in attrs_cond])

            parent_record2: dict[ast.RelExpr, ast.RelExpr] = {node: parent}
            queue = [node]
            while len(queue) > 0:
                node = queue.pop(0)
                for input in node.inputs:
                    parent_record2[input] = node
                    # print("Input: ", input)
                    match type(input):
                        case ast.RelRef:
                            rel = str(input)
                            attrs = [
                                attr
                                for attr in attrs_cond
                                if type(attr) == ast.AttrRef
                                and (attr.rel == rel or attr.rel == None)
                                and attr.name in dd[rel].keys()
                            ]
                            if len(attrs) == 0:
                                continue
                        case ast.Rename:
                            dd[input.relname] = dd[str(input.inputs[0])]
                            attrs = [
                                attr
                                for attr in attrs_cond
                                if type(attr) == ast.AttrRef
                                and (attr.rel == input.relname or attr.rel == None)
                                and attr.name in dd[input.relname].keys()
                            ]
                            if len(attrs) == 0:
                                continue
                        case ast.Cross:
                            rels = []
                            for rel in input.inputs:
                                match type(rel):
                                    case ast.RelRef:
                                        rels.append(str(rel))
                                    case ast.Rename:
                                        dd[rel.relname] = dd[str(rel.inputs[0])]
                                        rels.append(rel.relname)
                            if len(rels) == 2:
                                attrs1 = [
                                    attr
                                    for attr in attrs_cond
                                    if type(attr) == ast.AttrRef
                                    and (attr.rel == rels[0] or attr.rel == None)
                                    and attr.name in dd[rels[0]].keys()
                                ]
                                attrs2 = [
                                    attr
                                    for attr in attrs_cond
                                    if type(attr) == ast.AttrRef
                                    and (attr.rel == rels[1] or attr.rel == None)
                                    and attr.name in dd[rels[1]].keys()
                                ]
                                if len(attrs1) == 0 or len(attrs2) == 0:
                                    queue.append(input)
                                    continue
                            else:
                                queue.append(input)
                                continue

                    new_parent = parent_record2[input]
                    node = ast.Select(cond=cond, input=input)
                    # print("Node: ", node)
                    # Replace input parent's child with new select node
                    for i, _input in enumerate(new_parent.inputs):
                        if input == new_parent.inputs[i]:
                            new_parent.inputs[i] = node
                    if input in parent_record.keys():
                        parent_record[input] = node
                    # Replace origin parent's child with origin's child
                    if parent != None:
                        for i, _input in enumerate(parent.inputs):
                            if origin == parent.inputs[i]:
                                parent.inputs[i] = origin.inputs[0]
                    else:
                        ra = origin.inputs[0]
                    if origin.inputs[0] in parent_record.keys():
                        parent_record[origin.inputs[0]] = parent

    return ra


def rule_merge_selections(ra: ast.RelExpr) -> ast.RelExpr:
    # child, parent
    parent_record: dict[ast.RelExpr, ast.RelExpr] = {ra: None}
    while len(parent_record) > 0:
        node = list(parent_record.keys())[0]
        parent = parent_record.pop(node)

        for input in node.inputs:
            parent_record[input] = node

        if type(node) == ast.Select:
            conds = []
            origin = node

            while type(node) == ast.Select:
                conds.append(node.cond)
                node = node.inputs[0]

            cond = conds.pop(0)
            while len(conds) > 0:
                cond = ast.ValExprBinaryOp(
                    left=cond, op=parse.RAParser.AND, right=conds.pop(0)
                )
            node = ast.Select(cond=cond, input=node)

            # Replace input node in parent
            if parent != None:
                for i, _input in enumerate(parent.inputs):
                    if origin == parent.inputs[i]:
                        parent.inputs[i] = node
            else:
                ra = node

    return ra


def rule_introduce_joins(ra: ast.RelExpr) -> ast.RelExpr:
    # child, parent
    parent_record: dict[ast.RelExpr, ast.RelExpr] = {ra: None}
    while len(parent_record) > 0:
        node = list(parent_record.keys())[0]
        parent = parent_record.pop(node)

        for input in node.inputs:
            parent_record[input] = node
        # Find a selection
        if type(node) == ast.Select:
            origin = node
            cond = node.cond

            input = node.inputs[0]
            if type(input) == ast.Cross:
                node = ast.Join(left=input.inputs[0], cond=cond, right=input.inputs[1])
                # print(node)

                # Replace input node in parent
                if parent != None:
                    for i, _input in enumerate(parent.inputs):
                        if origin == parent.inputs[i]:
                            parent.inputs[i] = node
                else:
                    ra = node
                parent_record[node] = parent

    return ra


if __name__ == "__main__":
    dd = {}
    dd["Person"] = {"name": "string", "age": "integer", "gender": "string"}
    dd["Eats"] = {"name": "string", "pizza": "string"}
    dd["Serves"] = {"pizzeria": "string", "pizza": "string", "price": "integer"}
    dd["Frequents"] = {"name": "string", "pizzeria": "string"}

    # stmt = """Eats \cross \select_{Person.gender='f' and Person.age=16 and Person.firstName = 'first' and Person.lastName = 'last'}
    #             (Person \cross \select_{Serves.name = 'pizza' and Serves.price = 10 } Serves);"""

    # stmt = """\select_{Eats.pizza = Serves.pizza} \select_{Person.name = Eats.name}
    #             ((Person \cross Eats) \cross Serves);"""
    # stmt = "\select_{price < 10} ((Person \cross Eats) \cross Serves);"
    # stmt = """\select_{Eats1.pizza = Eats2.pizza} \select_{Eats2.name = 'Amy'} (\\rename_{Eats1: *}(Eats)
    #                    \cross \\rename_{Eats2: *}(Eats));"""

    # stmt = "Pizzeria \cross (\select_{pizza = 'mushroom'} \select_{price = 10} Serves);"
    # stmt = "\select_{name = 'Amy'} \select_{gender = 'f'} \select_{age = 16} Person;"
    stmt = """\select_{Eats.pizza = Serves.pizza}((\select_{Person.name = Eats.name}
                       (Person \cross Eats)) \cross Serves);"""

    ra = parse.one_statement_from_string(stmt)
    result = rule_introduce_joins(ra)
    print(result)
