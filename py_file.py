self.mapper.map({1})
if self.mapper.error_message:
    raise LogicianException(self.mapper.error_message, self.node, self.inspector)
self.mapper.get_connection_string()
connection = pg.connect(self.mapper.connect_string)
sql_select = [{4}]
where_info = {}
where_chain = {}
{0}
sql_where = self.mapper._build_dataframe_where({1}, where_info, where_chain)
sql = self.mapper.get_df_sql({1}, sql_select, sql_where)
if self.mapper.error_message:
    raise LogicianException(self.mapper.error_message, self.node, self.inspector)
{3} = pd.read_sql_query(sql, connection, coerce_float=False)
if sql_select:
    {3}.columns = self.mapper.specific_panda_columns[{1}].split(',')
else:
    {3}.columns = self.mapper.panda_column_names[{1}].replace("'",'').split(',')
self.mapper.manage_dates({5}, {3})

ss