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
    complete: set[ast.Select] = set()
    while len(parent_record) > 0:
        node = list(parent_record.keys())[0]
        parent = parent_record.pop(node)

        for input in node.inputs:
            parent_record[input] = node
        # Find a selection
        if type(node) == ast.Select:
            if node in complete:
                continue
            origin = node
            cond = node.cond
            attrs_cond: list[ast.AttrRef | ast.RAString | ast.RANumber] = cond.inputs
            # print("Attrs: ", [str(x) for x in attrs_cond])

            parent_record2: dict[ast.RelExpr, ast.RelExpr] = {node: parent}
            queue = [node]
            while len(queue) > 0:
                node = queue.pop(0)
                for input in node.inputs:
                    parent_record2[input] = node
                    # print("Input: ", input)
                    attrs = []
                    match type(input):
                        case ast.RelRef:
                            rel = str(input)
                            attrs += [
                                attr
                                for attr in attrs_cond
                                if type(attr) == ast.AttrRef
                                and (attr.rel == rel or attr.rel == None)
                                and attr.name in dd[rel].keys()
                            ]
                        case ast.Rename:
                            dd[input.relname] = dd[str(input.inputs[0])]
                            attrs += [
                                attr
                                for attr in attrs_cond
                                if type(attr) == ast.AttrRef
                                and (attr.rel == input.relname or attr.rel == None)
                                and attr.name in dd[input.relname].keys()
                            ]
                        case ast.Cross:
                            attrs1 = []
                            attrs2 = []
                            left_rels = []
                            right_rels = []
                            left_queue: list[ast.RelExpr] = [input.inputs[0]]
                            right_queue: list[ast.RelExpr] = [input.inputs[1]]
                            while len(left_queue) > 0:
                                child = left_queue.pop(0)
                                match type(child):
                                    case ast.RelRef:
                                        left_rels.append(str(child))
                                    case ast.Rename:
                                        dd[child.relname] = dd[str(child.inputs[0])]
                                        left_rels.append(child.relname)
                                    case _:
                                        left_queue += child.inputs
                            while len(right_queue) > 0:
                                child = right_queue.pop(0)
                                match type(child):
                                    case ast.RelRef:
                                        right_rels.append(str(child))
                                    case ast.Rename:
                                        dd[child.relname] = dd[str(child.inputs[0])]
                                        right_rels.append(child.relname)
                                    case _:
                                        right_queue += child.inputs

                            # Test if both attributes appear
                            for rel in left_rels:
                                attrs1 += [
                                    attr
                                    for attr in attrs_cond
                                    if type(attr) == ast.AttrRef
                                    and (attr.rel == rel or attr.rel == None)
                                    and attr.name in dd[rel].keys()
                                ]
                            for rel in right_rels:
                                attrs2 += [
                                    attr
                                    for attr in attrs_cond
                                    if type(attr) == ast.AttrRef
                                    and (attr.rel == rel or attr.rel == None)
                                    and attr.name in dd[rel].keys()
                                ]
                            # print(
                            #     "Attrs1: ",
                            #     [str(x) for x in attrs1],
                            #     "Attrs2: ",
                            #     [str(x) for x in attrs2],
                            # )
                            if len(attrs1) == 0 or len(attrs2) == 0:
                                queue.append(input)
                                continue
                            else:
                                attrs.append(attrs1)
                                attrs.append(attrs2)
                    if len(attrs) == 0:
                        queue.append(input)
                        continue

                    new_parent = parent_record2[input]
                    new_node = ast.Select(cond=cond, input=input)
                    # print("Node: ", new_node, "\n")
                    # Replace input parent's child with new select node
                    for i, _input in enumerate(new_parent.inputs):
                        if input == new_parent.inputs[i]:
                            new_parent.inputs[i] = new_node
                    if input in parent_record.keys():
                        parent_record[input] = new_node

                    # Replace origin parent's child with origin's child
                    if parent != None:
                        for i, _input in enumerate(parent.inputs):
                            if origin == parent.inputs[i]:
                                parent.inputs[i] = origin.inputs[0]
                    else:
                        ra = origin.inputs[0]
                    if origin.inputs[0] in parent_record.keys():
                        parent_record[origin.inputs[0]] = parent

                    complete.add(new_node)

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

    stmt = "\project_{P1.name, P2.name} \select_{P1.age = 16 and P2.age = 16 and P1.name = P2.name} ((\\rename_{P1: *} Person) \cross (\\rename_{P2: *} Person));"
    # stmt = "\project_{A.name, B.name} (\select_{A.pizza = B.pizza} ((\\rename_{A: *} Eats) \cross (\\rename_{B: *} Eats)));"
    ra = parse.one_statement_from_string(stmt)
    ra = rule_break_up_selections(ra)
    ra = rule_push_down_selections(ra, dd)
    ra = rule_merge_selections(ra)
    ra = rule_introduce_joins(ra)
    print(ra)
