"""

  """

from githubdata import GithubData
from mirutil.df_utils import save_df_as_a_nice_xl as snxl


class GDUrl :
    cur = 'https://github.com/imahdimir/g-d-TSETMC_ID-2-FirmTicker'
    trg = 'https://github.com/imahdimir/d-TSETMC_ID-2-FirmTicker'
    mb2f = 'https://github.com/imahdimir/d-multi-BaseTickers-2-same-FirmTicker'

gu = GDUrl()

class ColName :
    tid = 'TSETMC_ID'
    ftic = 'FirmTicker'

c = ColName()

def main() :
    pass

    ##

    gd_trg = GithubData(gu.trg)
    gd_trg.overwriting_clone()
    ##
    dft = gd_trg.read_data()
    ##
    dft = dft.astype(str)
    ##
    dft = dft.drop_duplicates()
    assert dft[c.tid].is_unique

    ##
    msk = dft[c.ftic].str.contains('پذيره')
    df1 = dft[msk]
    assert df1.empty

    ##

    gd_mb2f = GithubData(gu.mb2f)
    gd_mb2f.overwriting_clone()
    ##
    dfz = gd_mb2f.read_data()
    ##
    dfz = dfz.astype(str)
    ##
    dfz.set_index(c.tid , inplace = True)
    ##

    dft['ft'] = dft[c.tid].map(dfz[c.ftic])
    ##
    msk = dft['ft'].notna()
    df1 = dft[msk]
    ##
    dft.loc[msk , c.ftic] = dft['ft']
    ##
    dft = dft[[c.tid , c.ftic]]
    ##

    dft.sort_values(c.ftic , inplace = True)
    ##
    dftp = gd_trg.data_fp
    snxl(dft , dftp)
    ##
    msg = 'governed by: '
    msg += gu.cur
    ##

    gd_trg.commit_and_push(msg)

    ##

    gd_trg.rmdir()
    gd_mb2f.rmdir()

    ##

##
if __name__ == '__main__' :
    main()

##
