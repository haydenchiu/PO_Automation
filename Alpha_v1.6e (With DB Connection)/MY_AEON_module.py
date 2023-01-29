import os
import pandas as pd
import numpy as np
from pathlib import Path
import importlib
import getpass
import datetime
import time
import shutil
import uuid
import config
import re

# Import Customer inofrmation from B1
username = getpass.getuser()
Market = '067 Malaysia'
MLA = 'AEON'

#'PO NUMBER':'PURCHASE ORDER','QTY CASE':'Packing','COST UNIT PRICE':'Unit Price','COST AMOUNT':'Amount'
# 'UUID','SALES ORDER','ORDER DATE','DELIVERY DATE',
#                     'BP Code','BP Name','STORE CODE','STORE NAME',
#                     'PO NUMBER','Simplified Commercial Code',
#                     'CMMF','QTY CASE','UOM','COST UNIT PRICE','ORDER QTY',
#                     'Line Quantity (pc)','COST AMOUNT','WAREHOUSE',
#                     'Remarks-1: Possible CMMF','Remarks-2: Respective Commercial Code',
#                     'Remarks-3: Respective Description','Remarks-4: Respective CMMF Type',
#                     'Remarks-5: Respective Status'

dtw_col_2_db_dic = {
    'UUID':'so_line_uuid','SALES ORDER':'salesorder',
    'ORDER DATE':'order_date','DELIVERY DATE':'delivery_date',
    'BP Code':'bp_code','BP Name':'bp_name',
    'STORE CODE':'store_code','STORE NAME':'store_name',
    'PURCHASE ORDER':'purchase_order',
    'Simplified Commercial Code':'commercial_code','CMMF':'cmmf',
    'Packing':'packing','UOM':'uom',
    'Unit Price':'unit_price','ORDER QTY':'delivery_qty',
    'Line Quantity (pc)':'line_qty_pc','Amount':'line_value',
    'WAREHOUSE':'warehouse','Remarks-1: Possible CMMF':'remarks_1_cmmf',
    'Remarks-2: Respective Commercial Code':'remarks_2_cc','Remarks-3: Respective Description':'remarks_3_desc',
    'Remarks-4: Respective CMMF Type':'remarks_4_cmmf_type','Remarks-5: Respective Status':'remarks_5_status'
    }

dtw_col_type_2_db_dic = {
    'SALES ORDER':'Int64',
    'CMMF':'Int64',
    'Packing':'Int64'
    }


def read_store_map():
    # Import NTUC Internal Store ID and B1 BP Code Mapping
    os.chdir(fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\02 SAP Business One Mapping')
    aeon_store_map_df = pd.read_excel('Aeon_SB1_store_map.xlsx',header=0)
    aeon_store_map_df.dropna(subset=['Store Code'],inplace=True)
    aeon_store_map_df['Store Code'] = aeon_store_map_df['Store Code'].astype(np.int64)
    return(aeon_store_map_df)

def read_AEON_cmmf_map():
    # Import GS Malaysia maintains Aeon CMMF mapping table
    os.chdir(fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\02 SAP Business One Mapping')
    mla_seb_sda_cmmf_map_df = pd.read_excel('Aeon cmmf SDA map.xlsx')
    mla_seb_cw_cmmf_map_df = pd.read_excel('Aeon cmmf CW map.xlsx')

    mla_seb_cw_cmmf_map_df.rename(columns={'AEON Code':'AEON ITEM CODE',
                                        'Aeon Bar Code':'AEON BARCODE',
                                        'SEB CMMF':'CMMF',
                                        'PRODUCT DESCRIPTION':'Product Name'},inplace=True)

    mla_seb_cw_cmmf_map_df = mla_seb_cw_cmmf_map_df.dropna(subset=['AEON ITEM CODE','AEON BARCODE','CMMF'],how='all')

    mla_seb_cw_cmmf_map_df['AEON ITEM CODE'] = mla_seb_cw_cmmf_map_df['AEON ITEM CODE'].fillna(method='ffill')

    mla_seb_sda_cmmf_map_df = mla_seb_sda_cmmf_map_df.dropna(subset=['AEON ITEM CODE','AEON BARCODE','CMMF'],how='all')

    mla_seb_sda_cmmf_map_df['AEON ITEM CODE'] = mla_seb_sda_cmmf_map_df['AEON ITEM CODE'].fillna(method='ffill')

    mla_seb_cmmf_map_df = pd.concat([mla_seb_sda_cmmf_map_df,mla_seb_cw_cmmf_map_df],ignore_index=True)

    mla_seb_cmmf_map_df = mla_seb_cmmf_map_df[(mla_seb_cmmf_map_df['CMMF'].notna())]

    mla_seb_cmmf_map_df['CMMF'] = mla_seb_cmmf_map_df['CMMF'].astype(str).astype(float).astype(np.int64)

    mla_seb_cmmf_map_df['AEON ITEM CODE'] = mla_seb_cmmf_map_df['AEON ITEM CODE'].astype(str).astype(float).astype(np.int64)

    mla_seb_cmmf_map_df.drop_duplicates(subset=['AEON ITEM CODE','CMMF'],inplace=True)

    mla_seb_cmmf_map_df = mla_seb_cmmf_map_df[(mla_seb_cmmf_map_df['AEON ITEM CODE'].astype(str).str.isnumeric())]
    return(mla_seb_cmmf_map_df)


def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return(folder_path)


def dtw_2_db_template(pre_or_post_edit_pre_dtw_df):
    #This function transforms pre edit or post edit dtw into po_automation database format
    df = pre_or_post_edit_pre_dtw_df
    df = df.astype(dtw_col_type_2_db_dic)
    df.rename(columns=dtw_col_2_db_dic,inplace=True)
    session_id = [config.s_id]*df.shape[0]
    df.insert(0, 'session_id', session_id)
    return(df)



def create_KAM_temp_excel(pi2_df, sq004_df, aeon_store_map_df, input_dir, po_files):
    
    #input_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\01 PO Data Source\01 Aeon'
    #output_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\04 Pre-dtw'
    archive_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\04 Pre-dtw\01 Archives'
    blk_box_02_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\01 Aeon\02 Untouched Pre-dtw'

    config.refresh_session_id() #create new session id for pre-dtw
    config.refresh_today() #obtain most updated datetime string
    

    # read Aeon PO xls and append batches into one df
    os.chdir(input_dir)
    for n, f in enumerate(po_files):
        #df = pd.concat([pd.read_excel(f,header=1,skipfooter=1) for f in sorted(list(Path(input_dir).rglob('*.xls')) + list(Path(input_dir).rglob('*.xlsx')), key=lambda x: x.stem)],sort = False).reset_index(drop=True)
        ddf = pd.read_excel(f,header=1,skipfooter=1)#.reset_index(drop=True)
        if n == 0:
            df = ddf
            #print(df.shape)
        else:
            df = df.append(ddf)
            #print(df.shape)
    
    df.columns = [x.upper() for x in df.columns]
    
    #calculated field
    df['Line Quantity (pc)'] = df['QTY CASE'] * df['ORDER QTY']
    
    #Logic: the first string has digits (could have Alphabets, -, ., _)
    pat = r'([A-Za-z]+[\d@]+[\w@]*|[\d@]+[A-Za-z]+[\w@]*)'
    df['Simplified Commercial Code'] = df['ITEM DESCRIPTION'].apply(lambda x: re.search(pat,str(x).split()[0]).group() if re.search(pat,str(x).split()[0]) else np.nan)
    #print(df['Simplified Commercial Code'].unique())
    df['Simplified Commercial Code'] = df['Simplified Commercial Code'].astype(str)
    
    #display(df)
    
    
    ### Keep track of original PO EAN order
    df['PO_Store_EAN_Key'] = df['PO NUMBER'].astype(str) + df['STORE CODE'].astype(str) + df['ITEM NO'].astype(str)
    
    po_store_ean_order = list(df['PO_Store_EAN_Key'].unique())
    
    df['PO_Store_EAN_Key'] = pd.Categorical(df['PO_Store_EAN_Key'],po_store_ean_order)
    ###
    
    
    # Join Pi2, sq004 for EAN Code and CMMF Matching
    
    # left join pi2 ['CMMF Code', 'Commercial Code']
    p_df = pi2_df.copy()

    #p_df = pi2_df[pi2_df['Commercial Code'].notnull()]
    
    #p_df = pi2_df[pi2_df['Simplified Commercial Code'].notnull()]
    
    p_df['Simplified Commercial Code'] = p_df['Simplified Commercial Code'].astype(str)
    
    p_df['CMMF Code'] = p_df['CMMF Code'].astype(str)
    
    p_df = p_df[p_df['CMMF Code'].str.isnumeric()]
    
    p_df['CMMF Code'] = p_df['CMMF Code'].astype(np.int64)
    
    p_df['Status'] = p_df['Gen. status'].str.split(' - ').str[0]
    
    cond_p_df = p_df[['CMMF Code','Commercial Code','Simplified Commercial Code','CMMF description','CMMF Type','Status']]
    
    cond_p_df['Status'] = cond_p_df['Status'].astype(np.int64)
    

    cond_p_df = cond_p_df[(cond_p_df['CMMF Type']=='A')|
                          (cond_p_df['CMMF Type']=='B')|
                          (cond_p_df['CMMF Type']=='D')] #only Finished Good, Accessories, and Set
    
    cond_p_df.drop_duplicates(keep='first', inplace=True)
    
    
    # sq004_df left join pi2
    
    sq_df = sq004_df[['Product','Location']]
    
    sq_pi_df = sq_df.merge(cond_p_df[['CMMF Code','Commercial Code','Simplified Commercial Code','CMMF description','CMMF Type','Status']],how='left',left_on=['Product'],right_on=['CMMF Code'])
    
    sq_pi_df.drop_duplicates(keep='first',inplace=True)
    
    #display(sq_pi_df)
    
    # mla_seb_cmmf_map_df left join pi2
    mla_seb_cmmf_map_df = read_AEON_cmmf_map()
    mla_cmmf_df = mla_seb_cmmf_map_df[['CMMF','AEON ITEM CODE']]
    
    mla_pi_df = mla_cmmf_df.merge(cond_p_df[['CMMF Code','Commercial Code','Simplified Commercial Code','CMMF description','CMMF Type','Status']],how='left',left_on=['CMMF'],right_on=['CMMF Code'])
    
    mla_pi_df.drop_duplicates(keep='first',inplace=True)
    
    #display(mla_pi_df)
    
    #merge for logistic data    
    #case 1 (mapping on AEON ITEM CODE)-------------
    df_1 = df.merge(mla_pi_df[['AEON ITEM CODE','CMMF Code','Commercial Code','CMMF description','CMMF Type','Status']],how='left',left_on=['ITEM NO'],right_on=['AEON ITEM CODE'])
    
    df_1.drop_duplicates(keep='first', inplace=True)
    
    po_lines_2nd_mapping = list(df_1[df_1['CMMF Code'].isna()]['PO_Store_EAN_Key'].unique())
    
    #print(po_lines_2nd_mapping)
    
    df_1 = df_1[~df_1['PO_Store_EAN_Key'].isin(po_lines_2nd_mapping)]
    
    #display(df_1[df_1['CMMF Code'].isna()])
    
    #case 2 if CMMF is nan, map with Simplified Commercial Code
    
    df_2 = df[df['PO_Store_EAN_Key'].isin(po_lines_2nd_mapping)].merge(sq_pi_df[['CMMF Code','Commercial Code','Simplified Commercial Code','CMMF description','CMMF Type','Status']],how='left',on=['Simplified Commercial Code'])
    
    df_2.drop_duplicates(keep='first', inplace=True)
    
    #display(df_2[df_2['CMMF Code'].isna()])
    
    df = pd.concat([df_1,df_2],ignore_index=True)
    #-------------------------------------------------
    
        
    df['CMMF Code'] = df['CMMF Code'].astype('Int64')
    
    df['Status'] = df['Status'].astype('Int64')
    
    status_order_list = [40,50,60,65,70,80,25,20,15,10,90,110]
    
    df['Status'] = pd.Categorical(df['Status'],status_order_list)
    
    df.sort_values(by=['Status','CMMF Type'],inplace=True)
    
    df.sort_values(by=['PO_Store_EAN_Key'],inplace=True)
    
    #display(df)
    
    agg_d = {'CMMF Code': lambda x: list(x),
             'Commercial Code': lambda x: list(x),
             'Simplified Commercial Code': lambda x: list(x),
             'CMMF description': lambda x: list(x),
             'CMMF Type': lambda x: list(x),
             'Status': lambda x: list(x)}
    
    gp_pos_df = df.groupby(['PO NUMBER','STORE CODE','STORE NAME','ORDER DATE',
                            'DELIVERY DATE','ITEM DESCRIPTION','QTY CASE',
                            'UOM','ORDER QTY','COST UNIT PRICE','COST AMOUNT','Line Quantity (pc)'],sort=False).agg(agg_d).reset_index()
    
    
    gp_pos_df['CMMF'] = gp_pos_df['CMMF Code'].apply(lambda x: x[0] if isinstance(x,list) else x)
    
    gp_pos_df['CMMF'] = gp_pos_df['CMMF Code'].apply(lambda x: None if (isinstance(x,list))&(len(x)>1) else x[0])
    
    gp_pos_df['Simplified Commercial Code'] = gp_pos_df['Simplified Commercial Code'].apply(lambda x: x[0] if isinstance(x,list) else x)
    
    gp_pos_df['Simplified Commercial Code'] = gp_pos_df['Simplified Commercial Code'].apply(lambda x: None if x=='nan' else x)
    
    gp_pos_df.rename(columns={'CMMF Code':'Remarks-1: Possible CMMF',
                              'Commercial Code':'Remarks-2: Respective Commercial Code',
                              'CMMF description':'Remarks-3: Respective Description',
                              'CMMF Type':'Remarks-4: Respective CMMF Type',
                              'Status':'Remarks-5: Respective Status'},inplace=True)
    
    
    #display(gp_pos_df)
    
    
    
    # MARK 3: export DTW format
    
    gp_pos_df['Aeon Store ID'] = gp_pos_df['STORE CODE']
    
    #display(gp_pos_df)
    
    dtw_df = pd.merge(gp_pos_df,aeon_store_map_df,how='left',left_on='Aeon Store ID',right_on='Store Code')
    
    #display(dtw_df)
    
    # assign sudo sales order ID
    dtw_df['SALES ORDER'] = pd.factorize(dtw_df['PO NUMBER'])[0] + 1
    
    #print(dtw_df.columns)
    #display(dtw_df)
    
    #Change Dates to YYYYMMDD format
    #dtw_df['ORDER DATE'] = dtw_df['ORDER DATE'].apply(lambda x: datetime.datetime.strptime(x,'%d-%b-%y').strftime('%Y%m%d'))
    #dtw_df['DELIVERY DATE'] = dtw_df['DELIVERY DATE'].apply(lambda x: datetime.datetime.strptime(x,'%d-%b-%y').strftime('%Y%m%d'))
    
    dtw_df['UUID'] = [uuid.uuid1() for _ in range(len(dtw_df.index))]
    
    dtw_df['WAREHOUSE'] = np.nan
    
    dtw_df = dtw_df[['UUID','SALES ORDER','ORDER DATE','DELIVERY DATE',
                     'BP Code','BP Name','STORE CODE','STORE NAME',
                     'PO NUMBER','Simplified Commercial Code',
                     'CMMF','QTY CASE','UOM','COST UNIT PRICE','ORDER QTY',
                     'Line Quantity (pc)','COST AMOUNT','WAREHOUSE',
                     'Remarks-1: Possible CMMF','Remarks-2: Respective Commercial Code',
                     'Remarks-3: Respective Description','Remarks-4: Respective CMMF Type',
                     'Remarks-5: Respective Status']]
    
    #rename columns
    dtw_df.rename(columns={'PO NUMBER':'PURCHASE ORDER','QTY CASE':'Packing','COST UNIT PRICE':'Unit Price','COST AMOUNT':'Amount'},inplace=True)
    #display(dtw_df)
    
    output_dir = create_folder_if_not_exists(fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Data\16 Centralized PO Automation\{Market}\{MLA}\{config.today}\{config.s_id}')

    os.chdir(output_dir)
    dtw_df.to_excel('consolidated_pre_dtw_so.xlsx', index=False)

    #open directory window
    out_path = os.path.realpath(output_dir)
    os.startfile(out_path)

    #os.chdir(archive_dir)
    #t1 = datetime.datetime.now()
    #dtw_df.to_excel(f'consolidated_pre_dtw_so_{t1.strftime("%Y%m%d_%H%M")}.xlsx', index=False)

    os.chdir(blk_box_02_dir)
    dtw_df.to_excel(f'consolidated_pre_dtw_so_{config.s_id}.xlsx', index=False)
    
    return(dtw_df)

# def move_po_from_source_to_processed():
    
#     input_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\01 PO Data Source\01 Aeon'
#     output_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\05 Processed PO'
#     blk_box_01_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\01 Aeon\01 Source PO'
    
#     # read AEON PO xls and append batches into one df
#     os.chdir(input_dir)
    
#     for path in Path(input_dir).rglob('*.xls'):
#         print(path.name)

#         shutil.copy(fr'{input_dir}\{path.name}', fr'{blk_box_01_dir}\{path.name}')
#         shutil.move(fr'{input_dir}\{path.name}', fr'{output_dir}\{path.name}')



def finalize_dtw_template(ordr_dtw_df,rdr1_dtw_df):
    
    # initialize ORDR_df and RDR1_df
    ORDR_df = ordr_dtw_df
    RDR1_df = rdr1_dtw_df
    
    input_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Data\16 Centralized PO Automation\{Market}\{MLA}\{config.today}\{config.s_id}'
    output_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Data\16 Centralized PO Automation\{Market}\{MLA}\{config.today}\{config.s_id}'
    blk_box_03_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\01 {MLA}\03 Modified Pre-dtw'
    blk_box_04_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\01 {MLA}\04 Modified dtw'
    
    os.chdir(input_dir)
    dtw_df = pd.read_excel('consolidated_pre_dtw_so.xlsx')
    
    os.chdir(blk_box_03_dir)
    t2 = datetime.datetime.now()
    dtw_df.to_excel(f'consolidated_pre_dtw_so_{config.s_id}.xlsx',index=False)
    
    dtw_df = dtw_df[['UUID','SALES ORDER','ORDER DATE','DELIVERY DATE',
                     'BP Code','PURCHASE ORDER','CMMF','Unit Price',
                     'Line Quantity (pc)','Amount','WAREHOUSE']]
    
    
    dtw_df.rename(columns={'SALES ORDER':'DocNum','ORDER DATE':'DocDate',
                           'DELIVERY DATE':'DocDueDate','BP Code':'CardCode',
                           'PURCHASE ORDER':'NumAtCard','CMMF':'ItemCode','Unit Price':'Price',
                           'Line Quantity (pc)':'Quantity','Amount':'LineTotal',
                           'WAREHOUSE':'WhsCode'},inplace=True)
    
    dtw_df['DocDate'] = np.nan #Preset DocDate as empty to ensure the Docdate equals upload date

    ordr_df = dtw_df[['DocNum','DocDate','DocDueDate','CardCode','NumAtCard']]
    rdr1_df = dtw_df[['DocNum','ItemCode','Quantity','Price','WhsCode','LineTotal']]

    rdr1_df['WhsCode'] = rdr1_df['WhsCode'].fillna('').astype(str)
    rdr1_df['WhsCode'] = rdr1_df['WhsCode'].str.split('.').str[0]
    
    ordr_df.drop_duplicates(keep='first',inplace=True)
    
    #display(ordr_df)
    #display(rdr1_df)
    
    RDR1_top_level_headers = RDR1_df.columns.droplevel(1)
    ORDR_top_level_headers = ORDR_df.columns.droplevel(1)
    
    #print(list(RDR1_top_level_headers))
    
    RDR1_sec_level_headers = RDR1_df.columns.droplevel(0)
    ORDR_sec_level_headers = ORDR_df.columns.droplevel(0)
    
    RDR1_temp_df = pd.DataFrame(columns=RDR1_sec_level_headers)
    ORDR_temp_df = pd.DataFrame(columns=ORDR_sec_level_headers)
    
    
    ordr_df = ordr_df.merge(ORDR_temp_df,how='left')
    rdr1_df = rdr1_df.merge(RDR1_temp_df,how='left')
    
    ordr_df = ordr_df.reindex(columns=list(ORDR_temp_df.columns))
    rdr1_df = rdr1_df.reindex(columns=list(RDR1_temp_df.columns))
    
    rdr1_df.columns = pd.MultiIndex.from_tuples(zip(list(RDR1_top_level_headers), rdr1_df.columns))
    ordr_df.columns = pd.MultiIndex.from_tuples(zip(list(ORDR_top_level_headers), ordr_df.columns))
    
    #display(ordr_df)
    #display(rdr1_df)
    
    os.chdir(output_dir)
    ordr_df.to_csv('ordr_batch_upload.csv',index=False)
    rdr1_df.to_csv('rdr1_batch_upload.csv',index=False)

    os.chdir(blk_box_04_dir)
    ordr_df.to_csv(f'ordr_batch_upload_{config.s_id}.csv',index=False)
    rdr1_df.to_csv(f'rdr1_batch_upload_{config.s_id}.csv',index=False)

def pre_dtw_2_dtw_changes():
    blk_box_02_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\01 Aeon\02 Untouched Pre-dtw'
    blk_box_03_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\01 Aeon\03 Modified Pre-dtw'

    os.chdir(blk_box_02_dir)
    root_df = pd.read_excel(f'consolidated_pre_dtw_so_{config.s_id}.xlsx')

    os.chdir(blk_box_03_dir)
    current_df = pd.read_excel(f'consolidated_pre_dtw_so_{config.s_id}.xlsx')

    #calculated columns "Price Changed Line" & "Qty Changed Line"
    temp_df = pd.merge(current_df[['UUID','Unit Price','Line Quantity (pc)']],root_df[['UUID','Unit Price','Line Quantity (pc)']],how='left',on='UUID')
    temp_df['Price Changed'] = temp_df['Unit Price_x'] - temp_df['Unit Price_y']
    temp_df['Qty Changed'] = temp_df['Line Quantity (pc)_x'] - temp_df['Line Quantity (pc)_y']
    
    return(temp_df)


if __name__=='__main__':
    os.chdir(r'C:\Users\hchiu\Groupe SEB\Supply Chain Data Automation - Documents\Data\16 Centralized PO Automation\066 Singapore\NTUC\66fdfdad-f18a-441f-8205-c6da577bb768')
    pre_dtw_df = pd.read_excel('consolidated_pre_dtw_so.xlsx')
    dtw_2_db_template(pre_dtw_df).to_excel('test.xlsx',index=False)