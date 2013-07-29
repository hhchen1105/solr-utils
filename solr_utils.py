'''
Assumign solrpy is installed
'''

import solr

class SolrUtils:
  solr_info = { }
  solr_conn = None

  def __init__(self):
    self.__init_solr_connection()

  def is_this_a_setting_line(self, line):
    line = line.strip()
    if line == '':
      return False
    if line[0] == '#':
      return False
    return True

  def __init_solr_connection(self):
    self.solr_info = self.__get_solr_info()
    self.solr_conn = solr.SolrConnection(self.solr_info['host'])

  def simple_query(self, term, num_returns=20):
    batch_size = 20
    query_results = [ ]
    if num_returns <= batch_size:
      response = self.solr_conn.query(term, rows=num_returns)
      return [hit for hit in response.results]

    response = self.solr_conn.query(term, rows=batch_size)
    if isinstance(num_returns, str) and num_returns.lower() == 'all':
      num_returns = response.numFound
    while (response and num_returns > batch_size):
      query_results.extend([hit for hit in response.results])
      response = response.next_batch()
      num_returns -= batch_size
    query_results.extend([hit for hit in response.results[:num_returns]])
    return query_results

  def __get_solr_info(self):
    solr_info = { }
    f = open('./solr_settings', 'r')
    for line in f:
      if not self.is_this_a_setting_line(line):
        continue
      line = line.split('#')[0] # remove comments at the end of the line
      fields = line.strip().split(':')
      if len(fields) < 2:
        raise Exception("Wrong format in the solr setting file")
      field = fields[0].strip()
      val = ':'.join(fields[1:]).strip()
      if (val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'"):
        val = val[1:-1]
      solr_info[field] = val
    f.close()
    return solr_info

import nose.tools as nt

class TestAll():
  def test_foo(self):
    solr_utils = SolrUtils()
    returns = solr_utils.simple_query('chemical', 5)
    nt.assert_equal(len(returns), 5)
    returns = solr_utils.simple_query('chemical', 25)
    nt.assert_equal(len(returns), 25)
    returns = solr_utils.simple_query('chemical', 20)
    nt.assert_equal(len(returns), 20)
    returns = solr_utils.simple_query('chemical')
    nt.assert_equal(len(returns), 20)
    returns = solr_utils.simple_query('chemical', 'all')
    nt.assert_equal(len(returns), 37)

