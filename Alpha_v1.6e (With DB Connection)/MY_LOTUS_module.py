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
MLA = 'LOTUS'

#'PO NUMBER':'PURCHASE ORDER','QTY CASE':'Packing','COST UNIT PRICE':'Unit Price','COST AMOUNT':'Amount'
# 'UUID','SALES ORDER','ORDER DATE','DELIVERY DATE',
#                     'BP Code','BP Name','STORE CODE','STORE NAME',
#                     'PO NUMBER','Simplified Commercial Code',
#                     'CMMF','QTY CASE','UOM','COST UNIT PRICE','ORDER QTY',
#                     'Line Quantity (pc)','COST AMOUNT','WAREHOUSE',
#                     'Remarks-1: Possible CMMF','Remarks-2: Respective Commercial Code',
#                     'Remarks-3: Respective Description','Remarks-4: Respective CMMF Type',
#                     'Remarks-5: Respective Status'

raw_po_col_2_pre_dtw_dic = {
    'Delivery To GLN':'Store Code','Order No':'PURCHASE ORDER','Order Date':'ORDER DATE','Delivery Date/Time':'DELIVERY DATE',
    'GTIN':'EAN Code','Unit':'UOM',
    'Order Unit Price':'Unit Price ($)','Total Qty':'Line Quantity (pc)'
    }


dtw_col_2_db_dic = {
    'UUID':'so_line_uuid','SALES ORDER':'salesorder',
    'ORDER DATE':'order_date','DELIVERY DATE':'delivery_date',
    'BP Code':'bp_code','BP Name':'bp_name',
    'Store Code':'store_code','Delivery To Location Name':'store_name',
    'PURCHASE ORDER':'purchase_order',
    'EAN Code':'ean_code','CMMF':'cmmf',
    'Packing':'packing','UOM':'uom',
    'Unit Price ($)':'unit_price','Ordered Qty':'delivery_qty',
    'Line Quantity (pc)':'line_qty_pc','Amount ($)':'line_value',
    'WAREHOUSE':'warehouse','Remarks-1: Possible CMMF':'remarks_1_cmmf',
    'Remarks-2: Respective Commercial Code':'remarks_2_cc','Remarks-3: Respective Description':'remarks_3_desc',
    'Remarks-4: Respective CMMF Type':'remarks_4_cmmf_type','Remarks-5: Respective Status':'remarks_5_status'
    }

dtw_col_type_2_db_dic = {
    'SALES ORDER':'Int64',
    'EAN Code':'Int64',
    'CMMF':'Int64',
    'Packing':'Int64'
    }


def read_store_map():
    # Import LOTUS Internal Store ID and B1 BP Code Mapping
    os.chdir(fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\02 SAP Business One Mapping')
    lotus_store_map_df = pd.read_excel('Lotus_SB1_store_map.xlsx',header=0)
    lotus_store_map_df.dropna(subset=['Store Code'],inplace=True)
    lotus_store_map_df['Store Code'] = lotus_store_map_df['Store Code'].astype(np.int64)
    return(lotus_store_map_df)



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



def create_KAM_temp_excel(pi2_df, sq004_df, lotus_store_map_df, input_dir, po_files):
    
    #input_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\01 PO Data Source\02 Lotus'
    #output_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\04 Pre-dtw'
    archive_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\04 Pre-dtw\01 Archives'
    blk_box_02_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\02 {MLA}\02 Untouched Pre-dtw'

    config.refresh_session_id() #create new session id for pre-dtw
    config.refresh_today() #obtain most updated datetime string
    

    # read Lotus PO csv and append batches into one df
    os.chdir(input_dir)
    df = pd.DataFrame()
    
    for k, po_file in enumerate(po_files):
        
        ddf = pd.read_csv(po_file)
        #display(ddf)
        df = df.append(ddf)
        
    
    pos_df = df
    pos_df.drop(columns=['Store Code'],inplace=True)#to avoid duplicated column name
    #pos_df.info()
    #display(pos_df)
    #print(pos_df.columns)
        
    
    pos_df.rename(columns=raw_po_col_2_pre_dtw_dic,inplace=True)
    print(pos_df.columns)
    
    pos_df['Packing'] = np.where(pos_df['Ordered Qty']==0,np.nan,pos_df['Line Quantity (pc)']/pos_df['Ordered Qty'])
    
    
    pos_df['EAN Code'] = pos_df['EAN Code'].astype(float).astype(np.int64)
    pos_df['Store Code'] = pos_df['Store Code'].astype(np.int64)
    pos_df['PURCHASE ORDER'] = pos_df['PURCHASE ORDER'].astype(np.int64)
    
    #calculated field
    pos_df['Amount ($)'] = np.where(pos_df['Ordered Qty']==0,np.nan,pos_df['Unit Price ($)'] * pos_df['Ordered Qty'])
    
    
    
    pos_df['PO_Store_EAN_Key'] = pos_df['PURCHASE ORDER'].astype(str) + pos_df['Store Code'].astype(str) + pos_df['EAN Code'].astype(str)
    
    po_store_ean_order = list(pos_df['PO_Store_EAN_Key'].unique())
    
    #print(po_ean_order)
    
    pos_df['PO_Store_EAN_Key'] = pd.Categorical(pos_df['PO_Store_EAN_Key'],po_store_ean_order)
    
    
    # Join Pi2, sq004 for EAN Code and CMMF Matching
    
    # left join pi2 ['CMMF Code', 'EAN Code']
    
    p_df = pi2_df[pi2_df['EAN Code'].notnull()]
    
    p_df['EAN Code'] = p_df['EAN Code'].astype(str)
    
    p_df = p_df[p_df['EAN Code'].str.isnumeric()]
    
    p_df['EAN Code'] = p_df['EAN Code'].astype(np.int64)
    
    p_df['CMMF Code'] = p_df['CMMF Code'].astype(str)
    
    p_df = p_df[p_df['CMMF Code'].str.isnumeric()]
    
    p_df['CMMF Code'] = p_df['CMMF Code'].astype(np.int64)
    
    p_df['Status'] = p_df['Gen. status'].str.split(' - ').str[0]
    
    cond_p_df = p_df[['CMMF Code','Commercial Code','CMMF description','EAN Code','CMMF Type','Status']]
    print(cond_p_df['Status'].unique())
    cond_p_df['Status'] = cond_p_df['Status'].astype(int)
    
    cond_p_df = cond_p_df[(cond_p_df['CMMF Type']=='A')|
                          (cond_p_df['CMMF Type']=='B')|
                          (cond_p_df['CMMF Type']=='D')] #only Finished Good, Accessories, and Set
    
    cond_p_df.drop_duplicates(keep='first', inplace=True)
    
    
    # sq004_df left join pi2
    
    sq_df = sq004_df[['Product','Location']]
    
    sq_pi_df = sq_df.merge(cond_p_df[['CMMF Code','Commercial Code','CMMF description','EAN Code','CMMF Type','Status']],how='left',left_on=['Product'],right_on=['CMMF Code'])
    
    sq_pi_df.drop_duplicates(keep='first',inplace=True)
    
    #display(sq_pi_df)
    
    pos_df = pos_df.merge(sq_pi_df[['CMMF Code','Commercial Code','CMMF description','EAN Code','CMMF Type','Status']],how='left',on=['EAN Code'])
    
    pos_df.drop_duplicates(keep='first', inplace=True)
    
    #display(pos_df[pos_df['CMMF Code'].isna()])
        
    pos_df['CMMF Code'] = pos_df['CMMF Code'].astype('Int64')#.astype(np.int64)
    
    pos_df['Status'] = pos_df['Status'].astype('Int64')#.astype(int)
    
    status_order_list = [40,50,60,65,70,80,25,20,15,10,90,110]
    
    pos_df['Status'] = pd.Categorical(pos_df['Status'],status_order_list)
    
    pos_df.sort_values(by=['Status','CMMF Type'],inplace=True)
    
    #pos_df.sort_values(by=['PURCHASE ORDER','ORDER DATE','DELIVERY DATE'],inplace=True)
    
    pos_df.sort_values(by=['PO_Store_EAN_Key'],inplace=True)
    
    #display(pos_df)
    
    
    
    agg_d = {'CMMF Code': lambda x: list(x),
             'Commercial Code': lambda x: list(x),
             'CMMF description': lambda x: list(x),
             'CMMF Type': lambda x: list(x),
             'Status': lambda x: list(x)}
    
    gp_pos_df = pos_df.groupby(['PURCHASE ORDER','ORDER DATE','DELIVERY DATE',
                                'Store Code','Delivery To Location Name',
                                'EAN Code','Item Description','Packing','UOM','Unit Price ($)','Ordered Qty',
                                'Line Quantity (pc)','Amount ($)'],sort=False,dropna=False).agg(agg_d).reset_index()
    
    
    
    
    #dict.fromkeys(x).keys() create an ordered set
    #gp_pos_df['EAN Code'] = gp_pos_df['EAN Code'].apply(lambda x: list(dict.fromkeys(x).keys()))
    
    gp_pos_df['CMMF'] = gp_pos_df['CMMF Code'].apply(lambda x: None if (isinstance(x,list))&(len(x)>1) else x[0])
    
    gp_pos_df['CMMF Code'] = gp_pos_df['CMMF Code'].apply(lambda x: [e for e in x if pd.notnull(e)])
    
    gp_pos_df['Commercial Code'] = gp_pos_df['Commercial Code'].apply(lambda x: [e for e in x if pd.notnull(e)])
    
    gp_pos_df['CMMF description'] = gp_pos_df['CMMF description'].apply(lambda x: [e for e in x if pd.notnull(e)])
    
    gp_pos_df['CMMF Type'] = gp_pos_df['CMMF Type'].apply(lambda x: [e for e in x if pd.notnull(e)])
    
    gp_pos_df['Status'] = gp_pos_df['Status'].apply(lambda x: [e for e in x if pd.notnull(e)])
    
    
        
    gp_pos_df.rename(columns={'CMMF Code':'Remarks-1: Possible CMMF',
                              'Commercial Code':'Remarks-2: Respective Commercial Code',
                              'CMMF description':'Remarks-3: Respective Description',
                              'CMMF Type':'Remarks-4: Respective CMMF Type',
                              'Status':'Remarks-5: Respective Status'},inplace=True)
    
    #display(gp_pos_df)
    
    # MARK 3: export DTW format
    
    dtw_df = pd.merge(gp_pos_df,lotus_store_map_df,how='left',on=['Store Code'])
    
    
    # assign sudo sales order ID
    dtw_df['SALES ORDER'] = pd.factorize(dtw_df['PURCHASE ORDER'])[0] + 1
    
    #print(dtw_df.columns)
    #display(dtw_df)
    
#     #Change Dates to YYYYMMDD format
#     dtw_df['ORDER DATE'] = dtw_df['ORDER DATE'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d').strftime('%Y%m%d'))
#     dtw_df['DELIVERY DATE'] = dtw_df['DELIVERY DATE'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d').strftime('%Y%m%d'))
    
    dtw_df['UUID'] = [uuid.uuid1() for _ in range(len(dtw_df.index))]
    
    dtw_df['WAREHOUSE'] = np.nan
    
    
    dtw_df = dtw_df[['UUID','SALES ORDER','ORDER DATE','DELIVERY DATE',
                     'BP Code','BP Name','Store Code','Delivery To Location Name',
                     'PURCHASE ORDER','EAN Code','CMMF','Packing','UOM','Unit Price ($)','Ordered Qty',
                     'Line Quantity (pc)','Amount ($)','WAREHOUSE',
                     'Remarks-1: Possible CMMF','Remarks-2: Respective Commercial Code',
                     'Remarks-3: Respective Description','Remarks-4: Respective CMMF Type',
                     'Remarks-5: Respective Status']]
    
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
    blk_box_03_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\02 {MLA}\03 Modified Pre-dtw'
    blk_box_04_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\02 {MLA}\04 Modified dtw'
    
    os.chdir(input_dir)
    dtw_df = pd.read_excel('consolidated_pre_dtw_so.xlsx')
    
    os.chdir(blk_box_03_dir)
    t2 = datetime.datetime.now()
    dtw_df.to_excel(f'consolidated_pre_dtw_so_{config.s_id}.xlsx',index=False)
    
    dtw_df = dtw_df[['UUID','SALES ORDER','ORDER DATE','DELIVERY DATE',
                     'BP Code','PURCHASE ORDER','CMMF','Unit Price ($)',
                     'Line Quantity (pc)','Amount ($)','WAREHOUSE']]
    
    
    dtw_df.rename(columns={'SALES ORDER':'DocNum','ORDER DATE':'DocDate',
                           'DELIVERY DATE':'DocDueDate','BP Code':'CardCode',
                           'PURCHASE ORDER':'NumAtCard','CMMF':'ItemCode','Unit Price ($)':'Price',
                           'Line Quantity (pc)':'Quantity','Amount ($)':'LineTotal',
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
    blk_box_02_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\02 {MLA}\02 Untouched Pre-dtw'
    blk_box_03_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\15 Malaysia PO Automation\02 {MLA}\03 Modified Pre-dtw'

    os.chdir(blk_box_02_dir)
    root_df = pd.read_excel(f'consolidated_pre_dtw_so_{config.s_id}.xlsx')

    os.chdir(blk_box_03_dir)
    current_df = pd.read_excel(f'consolidated_pre_dtw_so_{config.s_id}.xlsx')

    #calculated columns "Price Changed Line" & "Qty Changed Line"
    temp_df = pd.merge(current_df[['UUID','Unit Price ($)','Line Quantity (pc)']],root_df[['UUID','Unit Price ($)','Line Quantity (pc)']],how='left',on='UUID')
    temp_df['Price Changed'] = temp_df['Unit Price ($)_x'] - temp_df['Unit Price ($)_y']
    temp_df['Qty Changed'] = temp_df['Line Quantity (pc)_x'] - temp_df['Line Quantity (pc)_y']
    
    return(temp_df)


if __name__=='__main__':
    os.chdir(r'C:\Users\hchiu\Groupe SEB\Supply Chain Data Automation - Documents\Data\16 Centralized PO Automation\066 Singapore\NTUC\66fdfdad-f18a-441f-8205-c6da577bb768')
    pre_dtw_df = pd.read_excel('consolidated_pre_dtw_so.xlsx')
    dtw_2_db_template(pre_dtw_df).to_excel('test.xlsx',index=False)