"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.df import save_df_as_a_nice_xl
from mirutil.ns import update_ns_module
from mirutil.ns import rm_ns_module

update_ns_module()
import ns

gdu = ns.GDU()
c = ns.Col()

def main() :
    pass

    ##

    # Get the target data
    gd_trg = GitHubDataRepo(gdu.trg)
    gd_trg.clone_overwrite()

    ##
    df_fp = gd_trg.data_fp
    df = pd.read_excel(df_fp , dtype = str)

    ##
    df = df.drop_duplicates()
    assert df[c.tse_id].is_unique

    ##
    msk = df[c.ftic].str.contains('پذيره')
    df1 = df[msk]
    assert df1.empty

    ##
    # Get the data of multi base tickers to the same firm ticker
    gd_mb2f = GitHubDataRepo(gdu.mbt)
    gd_mb2f.clone_overwrite()

    ##
    dfm_fp = gd_mb2f.data_fp
    dfm = pd.read_csv(dfm_fp , dtype = str)

    ##
    dfm = dfm.set_index(c.tse_id)

    ##
    nc = 'ft'
    df[nc] = df[c.tse_id].map(dfm[c.ftic])

    ##
    msk = df[nc].notna()
    df1 = df[msk]

    ##
    df.loc[msk , c.ftic] = df[nc]

    ##
    df = df.drop(columns = nc)

    ##
    # cols order
    df = df[[c.tse_id , c.ftic]]

    ##
    df = df.sort_values(c.ftic)

    ##
    save_df_as_a_nice_xl(df , df_fp)

    ##
    msg = 'governed by: '
    msg += gdu.slf
    print('commit msg is: ' , msg)

    ##
    gd_trg.commit_and_push(msg)

    ##
    gd_trg.rmdir()
    gd_mb2f.rmdir()

    ##
    rm_ns_module()

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
