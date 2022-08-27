##


from githubdata import GithubData
from mirutil.df_utils import read_data_according_to_type as rdata
from mirutil.df_utils import save_df_as_a_nice_xl as snxl


rp_url = 'https://github.com/imahdimir/d-TSETMC-ID-2-FirmTicker-map'
mb2ft_url = 'https://github.com/imahdimir/d-multi-BaseTickers-2-same-FirmTicker-map'

tid = 'TSETMC_ID'
ftic = 'FirmTicker'


def main() :
  pass

  ##
  rp = GithubData(rp_url)
  rp.clone()

  ##
  fp = rp.data_filepath
  df = rdata(fp)

  ##
  df = df.drop_duplicates()
  assert df[tid].is_unique

  ##
  msk = df[ftic].str.contains('پذيره')
  df1 = df[msk]
  assert df1.empty

  ##
  rp_m2f = GithubData(mb2ft_url)
  rp_m2f.clone()

  ##
  dfzfp = rp_m2f.data_filepath
  dfz = rdata(dfzfp)
  dfz[tid] = dfz[tid].astype(str)
  dfz = dfz.set_index(tid)

  ##
  df['ft'] = df[tid].map(dfz[ftic])
  msk = df['ft'].notna()
  df1 = df[msk]
  df.loc[msk , ftic] = df['ft']

  ##
  df = df[[tid , ftic]]

  ##
  df = df.sort_values(ftic)

  ##
  snxl(df , fp)

  ##
  cur_rp_url = 'https://github.com/' + rp.usr + '/gov-' + rp.repo_name
  ##
  msg = 'checked'
  msg += ' by: ' + cur_rp_url

  rp.commit_push(msg)

  ##

  rp.rmdir()
  rp_m2f.rmdir()


##


##