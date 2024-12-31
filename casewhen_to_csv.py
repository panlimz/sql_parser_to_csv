from sqlgpt_parser.parser.mysql_parser import parser as mysql_parser
import sqlgpt_parser.parser.tree.expression as E
import sqlgpt_parser.parser.tree.literal as L
import pandas as pd
import numpy as np

class RulesGeneratorVisitor:

    def print(self, n, s):
        print(4 * n * ' ', s)

    def parse(self, sql):
        self.p = mysql_parser.parse(sql)
        self.visit_query_body(self.p.query_body)

    def visit_query_body(self, node, n=0):
        self.visit_select(node.select, n + 1)

    def visit_select(self, node, n):
        for item in node.select_items:
            self.visit_select_item(item, n + 1)

    def visit_select_item(self, node, n):
        self.visit_alias(node.alias, n)
        self.visit_expression(node.expression, n)

    def visit_alias(self, node, n):
        self.alias = node[0]
        self.print(n, node[0])

    def visit_expression(self, node, n):
        self.visit_case(node, n + 1)

    def visit_case(self, node, n):
        dfs = []
        counter = 1
        for when_clause in node.when_clauses:
            when = self.visit_expr(when_clause.operand, n + 1)
            # When clause output
            if type(when_clause.result) == L.NullLiteral:
                out = ['S', self.alias, '=', 'NULL', None]
            elif type(when_clause.result) == E.QualifiedNameReference:
                out = ['S', self.alias, '=', str(when_clause.result.name), None]
            else:
                out = ['S', self.alias, '=', f'"{when_clause.result.value}"', None]

            df = self.parse_when_result(when, out, counter)
            counter += 1
            dfs.append(df)
        self.parse_data(dfs)

    def visit_expr(self, node, n):
        # When Clause Level
        match node:
            case E.ComparisonExpression():
                return self.visit_comparison_expr(node, n + 1)
            case E.LogicalBinaryExpression():
                return self.visit_logical_binary_expr(node, n + 1)
            case E.ListExpression():
                return self.visit_list_expr(node, n + 1)
            case E.InListExpression():
                return self.visit_in_list_expr(node)
            case E.InPredicate():
                return self.visit_in_predicate_expr(node, n + 1)
            case E.IsPredicate():
                return self.visit_is_predicate_expr(node, n + 1)
            case E.LikePredicate():
                return self.visit_like_predicate_expr(node, n + 1)

    def visit_logical_binary_expr(self, node, n):
        left = self.visit_expr(node.left, n + 1)
        right = self.visit_expr(node.right, n + 1)
        node_type = node.type
        if left:
            exp_res_1 = left
            exp_res_2 = right
            exp_res = exp_res_1 + [node_type] + exp_res_2
        else:
            exp_res = right + node_type
        return exp_res

    def visit_comparison_expr(self, node, n):
        left = str(node.left.name)
        if type(node.right) == L.StringLiteral:
            right = f'"{node.right.value}"'
        else:
            right = node.right.value
        node_type = node.type
        exp_res = [left, node_type, right]
        return exp_res

    def visit_in_predicate_expr(self, node, n):
        if node.is_not:
            cond = 'not in'
        else:
            cond = 'in'
        if type(node.value_list) == E.QualifiedNameReference:
            exp_res = [str(node.value.name), cond, (f'"{item.value}"' for item in node.value_list.values)]
        else:
            exp_res = [str(node.value.name), cond, (self.visit_expr(node.value_list, n + 1))]
        return exp_res

    def visit_is_predicate_expr(self, node, n):
        print(node)
        if node.is_not:
            cond = 'is not'
        else:
            cond = 'is'
        if node.kwd == 'null':
            exp_res = [str(node.value.name), cond, 'NULL']
        else:
            exp_res = [str(node.value.name), cond, f'"{node.kwd}"']
        return exp_res

    def visit_like_predicate_expr(self, node, n):
        if node.is_not:
            cond = 'not like'
        else:
            cond = 'like'
        exp_res = [str(node.value.name), cond, f'"{node.pattern.value}"']
        return exp_res

    def visit_list_expr(self, node, n):
        return self.visit_expr(node.values[0], n + 1)

    def visit_in_list_expr(self, node):
        # print(f"List: {node}")
        list_values = ()
        for node_item in node.values:
            list_values += (f'"{str(node_item.value)}"', )
        return list_values

    def parse_when_result(self, cond, res, counter):
        full_cond = []
        for i in range(0, len(cond), 4):
            if len(cond) < 3:
                full_cond.append(['C'] + cond)
            else:
                sub_cond = ['C'] + cond[i:i+4]
                full_cond.append(sub_cond)
        full_cond.append(res)
        df = pd.DataFrame(data=full_cond,
                          columns=['tp_regra', 'nm_campo', 'tp_operador', 'vl_regra', 'tp_operador_logico'])
        df['cd_regra'] = counter
        return df

    def parse_data(self, dfs):
        df = pd.concat(dfs)

        all_cols = ['ds_agrupamento', 'cd_regra', 'cd_sequencia', 'fl_elseif', 'tp_regra', 'nm_campo', 'tp_operador',
                    'vl_regra', 'tp_operador_logico', 'ds_regra', 'fl_regra_ativa']

        # Creating columns
        df['fl_regra_ativa'] = 'S'
        df['ds_agrupamento'] = 'Regra'
        df['cd_sequencia'] = df.groupby("cd_regra").cumcount() + 1
        df['fl_elseif'] = np.where(df['cd_regra'] == 1, 'N', 'S')

        for col in all_cols:
            if col not in df.columns:
                df[col] = None
        df = df[all_cols]
        df.to_csv(out_path, sep=';', header=True, index=False)


file_name = 'sql_file_name.sql'
src_path = f'in/{file_name}'
out_path = f'out/{file_name.split(".")[0]}.csv'

with open(src_path) as f:
    sql = f.read()
    v = RulesGeneratorVisitor()
    v.parse(sql)
