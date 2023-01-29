import os.path
import getpass
import importlib
import numpy as np
import uuid
import datetime
from openpyxl import load_workbook
import PySimpleGUI as sg
import pandas as pd
import MY_AEON_module
import MY_LOTUS_module
import base64_icons
import DB_connection
import config

username = getpass.getuser()
Market = '067 Malaysia'
mla_mod_dic = {'AEON':MY_AEON_module,'LOTUS':MY_LOTUS_module} #add more MY's mla modules here when they are ready!

#Black box log directory:
blk_box_log_dir = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Black Box\00 Logbook\01 PO Automation'



def read_pi2():
    # Import EAN Code mapping from Pi2
    os.chdir(fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Database\02 Matrix\05 Split & NON-Split Monthly CMMF level details\CMM_EAN_Code')
    pi2_df = pd.read_parquet('CMMF_Condensed_Matrix.parquet')
    return(pi2_df)

def read_sq004():
    # Import SQ004 for SKU Market Regisiteration
    os.chdir(fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\03 APO sq004')
    sq004_df = pd.read_excel('sq004.xlsx',header=0)
    sq004_df.dropna(subset=['Product'],inplace=True)
    sq004_df['Product'] = sq004_df['Product'].astype(np.int64)
    return(sq004_df)


def read_ordr_temp():
    # Import DTW excel template
    os.chdir(fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Database\01 Data Sources\04 B1')
    ordr_dtw_df = pd.read_excel('ORDR.xlt',header=[0,1])

    #Trim off trailing columns to avoid duplicated column name
    ordr_dtw_df = ordr_dtw_df.iloc[:,:90]
    return(ordr_dtw_df)

def read_rdr1_temp():
    # Import DTW excel template
    os.chdir(fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Database\01 Data Sources\04 B1')
    rdr1_dtw_df = pd.read_excel('RDR1.xlt',header=[0,1])

    #Trim off trailing columns to avoid duplicated column name
    rdr1_dtw_df = rdr1_dtw_df.iloc[:,:90]
    return(rdr1_dtw_df)

def place(elem):
    '''
    Places element provided into a Column element so that its placement in the layout is retained.
    :param elem: the element to put into the layout
    :return: A column element containing the provided element
    '''
    return sg.Column([[elem]], pad=(0,0))



def MY_PO_app():

    cur_mla = mla_mod_dic[list(mla_mod_dic.keys())[0]] #1st Key in mla_mod_dic
    #sg.theme('DarkAmber')

    header_row = [
        [
            sg.Text("Session ID: "), 
            sg.Multiline(f"{config.s_id}",size=(50,1),no_scrollbar=True,disabled=True,key="-SESSION ID-")
        ],
        [
            sg.Text("User ID: "), 
            sg.Text(f"{username}",key="-USERNAME-")
        ],
        [
            sg.Text("Market: "), 
            sg.Text("Malaysia",key="-MARKET-")
        ],
        [
            sg.Text("MLA: "),
            sg.Combo(list(mla_mod_dic.keys()), default_value='AEON',size=(10,1), enable_events=True ,key='-MLA COMBO-')
        ]
    ]

    file_list_column = [
        [
            sg.Text("Source PO Folder"),
            sg.In(size=(25, 1), enable_events=True, key="-FOLDER-"),
            sg.FolderBrowse(key="-BRWS PO BUT-"),
        ],
        [
            sg.Listbox(
                values=[], select_mode=sg.LISTBOX_SELECT_MODE_EXTENDED, enable_events=True, size=(40, 20), key="-FILE LIST-"
            )
        ],
    ]

    # For now will only show the name of the file that was chosen
    summary_viewer_column = [
        [sg.Text("System Message:")],
        [sg.Text("Number of MLA Source PO excel file engaged: "), 
        sg.Text(auto_size_text=False, text_color='black',key="-NBR OF PO-")], 
        [sg.HSeparator()],
        [sg.Text("Pre-DTW Summary:",background_color='orange')],
        [sg.Text("Total number of SO line: "),sg.Text(auto_size_text=False, text_color='black',key="-PRE DTW SO LINE-")],
        [sg.Text("Number of SO line with missing BP code:"),place(sg.Image(data=base64_icons.red_cross,visible=False,key="-PRE DTW BP CROSS-")),place(sg.Image(data=base64_icons.green_tick,visible=False,key="-PRE DTW BP TICK-")),sg.Text(auto_size_text=False, text_color='black',key="-PRE DTW MISS BP-")],
        [sg.Text("Number of SO line with unidentifiable CMMF: "),place(sg.Image(data=base64_icons.red_cross,visible=False,key="-PRE DTW CMMF CROSS-")),place(sg.Image(data=base64_icons.green_tick,visible=False,key="-PRE DTW CMMF TICK-")),sg.Text(auto_size_text=False, text_color='black',key="-PRE DTW MISS CMMF-")],
        [sg.Text("Total number of SO line with issue: "),place(sg.Image(data=base64_icons.red_cross,visible=False,key="-PRE DTW CROSS-")),place(sg.Image(data=base64_icons.green_tick,visible=False,key="-PRE DTW TICK-")),
        sg.Text(auto_size_text=False, text_color='black',key="-PRE DTW ISSUE LINE-")],
        [sg.HSeparator()],
        [sg.Text("DTW Summary:",background_color='orange')],
        [sg.Text("Total number of SO line: "),sg.Text(auto_size_text=False, text_color='black',key="-DTW SO LINE-")],
        [sg.Text("Number of SO line with missing BP code:"),place(sg.Image(data=base64_icons.red_cross,visible=False,key="-DTW BP CROSS-")),place(sg.Image(data=base64_icons.green_tick,visible=False,key="-DTW BP TICK-")),sg.Text(auto_size_text=False, text_color='black',key="-DTW MISS BP-")],
        [sg.Text("Number of SO line with unidentifiable CMMF: "),place(sg.Image(data=base64_icons.red_cross,visible=False,key="-DTW CMMF CROSS-")),place(sg.Image(data=base64_icons.green_tick,visible=False,key="-DTW CMMF TICK-")),sg.Text(auto_size_text=False, text_color='black',key="-DTW MISS CMMF-")],
        [sg.Text("Total number of SO line with issue: "),place(sg.Image(data=base64_icons.red_cross,visible=False,key="-DTW CROSS-")),place(sg.Image(data=base64_icons.green_tick,visible=False,key="-DTW TICK-")),sg.Text(auto_size_text=False, text_color='black',key="-DTW ISSUE LINE-")],
        [sg.HSeparator()],
        [sg.Multiline("Please select your designated MLA and use the 'Browse' button to find corresponding MLA PO excel.",size=(95,10),no_scrollbar=True,disabled=True,key="-MSG BOX-")]
    ]
    # User Interactable buttons
    buttons_column = [
        [sg.Text("Step 0: if necessary")],
        [sg.Button("Update SB1 Map", enable_events=True, key="-UPDATE SB1-")],
        [sg.Text("Step 1:")],
        [sg.Button('Create Pre-DTW', enable_events=True, disabled=True ,key="-PRE DTW BUT-"),sg.Button('Re-Select PO',enable_events=True, key="-RE SELECT PO BUT-")],
        [sg.Text("Step 2:")],
        [sg.Button('Create DTW', enable_events=True, disabled=True,key="-DTW BUT-")]
    ]
    # ----- Full layout -----
    layout = [
        [
            sg.Column(header_row,expand_x=False, expand_y=True)
        ],
        [
            sg.HSeparator(),
        ],
        [
            sg.Column(file_list_column,key="-FILE LIST COL-"),
            sg.VSeperator(),
            sg.Column(summary_viewer_column,expand_x=False, expand_y=True),
            sg.VSeperator(),
            sg.Column(buttons_column,justification='center',vertical_alignment='top')
        ]
    ]

    window = sg.Window("MY PO Automation Interface Alpha", layout, resizable=True)

    # Run the Event Loop
    while True:
        event, values = window.read()
        #print(event)
        print(values)
        if event == "Exit" or event == sg.WIN_CLOSED:
            break
        if event == '-MLA COMBO-':
            cur_mla = mla_mod_dic[values["-MLA COMBO-"]]
            importlib.reload(cur_mla)

        # Folder name was filled in, make a list of files in the folder
        if event == "-FOLDER-":
            folder = values["-FOLDER-"]
            try:
                # Get list of files in folder
                file_list = os.listdir(folder)
            except:
                file_list = []

            fnames = [
                f
                for f in file_list
                if os.path.isfile(os.path.join(folder, f))
                and f.lower().endswith((".xlsx", ".xls",".csv"))
            ]
            window["-FILE LIST-"].update(fnames)
        elif event == "-FILE LIST-":  # A file was chosen from the listbox
            try:
                window["-PRE DTW BUT-"].update(disabled=False)
                filename = os.path.join(
                    values["-FOLDER-"], values["-FILE LIST-"][0]
                )
                #print(filename)
                #window["-PO FILE NAME-"].update(filename)
                window["-NBR OF PO-"].update(len(values['-FILE LIST-']))
                window['-MSG BOX-'].update('Good things take time... The "Create Pre-DTW" process might take up to a mintue or two. \n(Please do not panic even if this program shows it is Not Responding. Patient is key.) \nPlease also be reminded that you should always save and close the modified Pre-DTW file before proceeding to "Create DTW."')

            except:
                pass

        elif event == '-PRE DTW BUT-':
            #try:
            window['-FILE LIST-'].update(disabled=True)
            window['-FOLDER-'].update(disabled=True)
            window['-BRWS PO BUT-'].update(disabled=True)
            window['-PRE DTW BUT-'].update(disabled=True)
            #reset summary display
            window['-PRE DTW SO LINE-'].update('')
            window['-PRE DTW MISS BP-'].update('')
            window['-PRE DTW MISS CMMF-'].update('')
            window['-PRE DTW ISSUE LINE-'].update('')
            window['-DTW SO LINE-'].update('')
            window['-DTW MISS BP-'].update('')
            window['-DTW MISS CMMF-'].update('')
            window['-DTW ISSUE LINE-'].update('')
            window['-MSG BOX-'].update('')
            window['-PRE DTW BP TICK-'].update(visible=False)
            window['-PRE DTW BP CROSS-'].update(visible=False)
            window['-PRE DTW CMMF TICK-'].update(visible=False)
            window['-PRE DTW CMMF CROSS-'].update(visible=False)
            window['-PRE DTW TICK-'].update(visible=False)
            window['-PRE DTW CROSS-'].update(visible=False)
            window['-DTW BP TICK-'].update(visible=False)
            window['-DTW BP CROSS-'].update(visible=False)
            window['-DTW CMMF TICK-'].update(visible=False)
            window['-DTW CMMF CROSS-'].update(visible=False)
            window['-DTW TICK-'].update(visible=False)
            window['-DTW CROSS-'].update(visible=False)

            if values['-FILE LIST-']:
                try:
                    df = pd.DataFrame([])
                    df = cur_mla.create_KAM_temp_excel(read_pi2(), read_sq004(), cur_mla.read_store_map(), values['-FOLDER-'], values['-FILE LIST-'])
                    print("Pre-DTW created!!!")
                    window['-SESSION ID-'].update(f"{config.s_id}")
                    window['-DTW BUT-'].update(disabled=False)

                    nbr_so_line = df.shape[0]
                    window['-PRE DTW SO LINE-'].update(nbr_so_line)
        
                    nbr_miss_bp = df['BP Code'].isna().sum()
                    window['-PRE DTW MISS BP-'].update(nbr_miss_bp)
                    if nbr_miss_bp==0:
                        window['-PRE DTW BP TICK-'].update(visible=True)
                    else:
                        window['-PRE DTW BP CROSS-'].update(visible=True)
                    
                    nbr_miss_cmmf = df['CMMF'].isna().sum()
                    window['-PRE DTW MISS CMMF-'].update(nbr_miss_cmmf)
                    if nbr_miss_cmmf==0:
                        window['-PRE DTW CMMF TICK-'].update(visible=True)
                    else:
                        window['-PRE DTW CMMF CROSS-'].update(visible=True)
                    
                    nbr_row_with_issue = df[['BP Code','CMMF']].isnull().any(axis=1).sum()
                    window['-PRE DTW ISSUE LINE-'].update(nbr_row_with_issue)
                    if nbr_row_with_issue==0:
                        window['-PRE DTW TICK-'].update(visible=True)
                    else:
                        window['-PRE DTW CROSS-'].update(visible=True)

                    #Log Book Writing
                    importlib.reload(DB_connection)
                    now = datetime.datetime.now()

                    dic_log = {'session_id':[config.s_id],'user_name':[username], 'market':['Malaysia'],'mla':[cur_mla.MLA],'exe_timestamp':[now],'process':['Raw PO to Pre-DTW'],'nbr_file':[len(values['-FILE LIST-'])],'nbr_so_line':[nbr_so_line],'nbr_error_line':[nbr_row_with_issue],'nbr_missing_bp':[nbr_miss_bp],'nbr_missing_cmmf':[nbr_miss_cmmf],'nbr_price_changed_line':[np.nan],'nbr_qty_changed_line':[np.nan]}
                    df_log = pd.DataFrame(dic_log)

                    DB_connection.copy_from_stringio(DB_connection.connect(DB_connection.params_dic), df_log, 'log_book')
                    #End of Logging

                    #Insert pre_dtw to pre_edit_pre_dtw
                    pre_edit_df = cur_mla.dtw_2_db_template(df)
                    DB_connection.copy_from_stringio(DB_connection.connect(DB_connection.params_dic), pre_edit_df, 'pre_edit_pre_dtw')
                    #End of inserting pre_dtw to pre_edit_pre_dtw

                    window['-MSG BOX-'].update('Pre-DTW file is created. Please cross check and fill in all the missing information.')
                
                except Exception as e:
                    print(e)
                    window['-MSG BOX-'].update('')
                    window['-MSG BOX-'].update(f'{e}')
                    pass
        elif event == '-RE SELECT PO BUT-':
            window['-PRE DTW SO LINE-'].update('')
            window['-PRE DTW MISS BP-'].update('')
            window['-PRE DTW MISS CMMF-'].update('')
            window['-PRE DTW ISSUE LINE-'].update('')
            window['-DTW SO LINE-'].update('')
            window['-DTW MISS BP-'].update('')
            window['-DTW MISS CMMF-'].update('')
            window['-DTW ISSUE LINE-'].update('')
            window['-MSG BOX-'].update('')
            window['-PRE DTW BP TICK-'].update(visible=False)
            window['-PRE DTW BP CROSS-'].update(visible=False)
            window['-PRE DTW CMMF TICK-'].update(visible=False)
            window['-PRE DTW CMMF CROSS-'].update(visible=False)
            window['-PRE DTW TICK-'].update(visible=False)
            window['-PRE DTW CROSS-'].update(visible=False)
            window['-DTW BP TICK-'].update(visible=False)
            window['-DTW BP CROSS-'].update(visible=False)
            window['-DTW CMMF TICK-'].update(visible=False)
            window['-DTW CMMF CROSS-'].update(visible=False)
            window['-DTW TICK-'].update(visible=False)
            window['-DTW CROSS-'].update(visible=False)

            #refresh file list in case file names are changed during pre-dtw process
            try:
                window['-FILE LIST-'].update(disabled=False)
                window['-FOLDER-'].update(disabled=False)
                window['-BRWS PO BUT-'].update(disabled=False)

                window["-FILE LIST-"].update(values=[])
                folder = values["-FOLDER-"]
                try:
                    # Get list of files in folder
                    file_list = os.listdir(folder)
                except:
                    file_list = []

                fnames = [
                    f
                    for f in file_list
                    if os.path.isfile(os.path.join(folder, f))
                    and f.lower().endswith((".xlsx", ".xls",".csv"))
                ]
                window["-FILE LIST-"].update(fnames)

                #need to reset global s_id here**********************

                window['-SESSION ID-'].update("0")
            except:
                pass

        elif event == '-DTW BUT-':
            window['-DTW SO LINE-'].update('')
            window['-DTW MISS BP-'].update('')
            window['-DTW MISS CMMF-'].update('')
            window['-DTW ISSUE LINE-'].update('')
            window['-MSG BOX-'].update('')
            window['-DTW BP TICK-'].update(visible=False)
            window['-DTW BP CROSS-'].update(visible=False)
            window['-DTW CMMF TICK-'].update(visible=False)
            window['-DTW CMMF CROSS-'].update(visible=False)
            window['-DTW TICK-'].update(visible=False)
            window['-DTW CROSS-'].update(visible=False)

            try:
                cwd_path = fr'C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Data\16 Centralized PO Automation\{cur_mla.Market}\{cur_mla.MLA}\{config.today}\{config.s_id}'
                os.chdir(cwd_path)
                df = pd.DataFrame([])
                df = pd.read_excel('consolidated_pre_dtw_so.xlsx')

                cur_mla.finalize_dtw_template(read_ordr_temp(),read_rdr1_temp())
                print('DTW Upload Files are ready!!!')
                delta_df = cur_mla.pre_dtw_2_dtw_changes()

                nbr_changed_price = delta_df[delta_df['Price Changed']!=0].shape[0]

                nbr_changed_qty = delta_df[delta_df['Qty Changed']!=0].shape[0]
        
                nbr_so_line = df.shape[0]
                window['-DTW SO LINE-'].update(nbr_so_line)
                
                nbr_miss_bp = df['BP Code'].isna().sum()
                window['-DTW MISS BP-'].update(nbr_miss_bp)
                if nbr_miss_bp==0:
                    window['-DTW BP TICK-'].update(visible=True)
                else:
                    window['-DTW BP CROSS-'].update(visible=True)
                
                nbr_miss_cmmf = df['CMMF'].isna().sum()
                window['-DTW MISS CMMF-'].update(nbr_miss_cmmf)
                if nbr_miss_cmmf==0:
                    window['-DTW CMMF TICK-'].update(visible=True)
                else:
                    window['-DTW CMMF CROSS-'].update(visible=True)
                
                nbr_row_with_issue = df[['BP Code','CMMF']].isnull().any(axis=1).sum()
                window['-DTW ISSUE LINE-'].update(nbr_row_with_issue)
                if nbr_row_with_issue==0:
                    window['-DTW TICK-'].update(visible=True)
                else:
                    window['-DTW CROSS-'].update(visible=True)

                #Log Book Writing
                now = datetime.datetime.now()

                # New data to write:
                dic_log = {'session_id':[config.s_id],'user_name':[username],'market':['Malaysia'],'mla':[cur_mla.MLA],'exe_timestamp':[now],'process':['Pre-DTW to DTW'],'nbr_file':[np.nan],'nbr_so_line':[nbr_so_line],'nbr_error_line':[nbr_row_with_issue],'nbr_missing_bp':[nbr_miss_bp],'nbr_missing_cmmf':[nbr_miss_cmmf],'nbr_price_changed_line':[nbr_changed_price],'nbr_qty_changed_line':[nbr_changed_qty]}

                df_log = pd.DataFrame(dic_log)
                DB_connection.copy_from_stringio(DB_connection.connect(DB_connection.params_dic), df_log, 'log_book')
                #End of Logging

                #Insert post_dtw to post_edit_pre_dtw
                post_edit_df = cur_mla.dtw_2_db_template(df)
                DB_connection.copy_from_stringio(DB_connection.connect(DB_connection.params_dic), post_edit_df, 'post_edit_pre_dtw')
                #End of inserting pre_dtw to post_edit_pre_dtw

                #declare output path
                path = os.path.realpath(cwd_path)

                if nbr_row_with_issue==0:
                    print('All issues are fixed!')
                    window['-MSG BOX-'].update(f'Successful! \nPlease find DTW csv files in the pop-up directory. \nOr you could find the file at: \n<{path}>')
                    #open directory window
                    os.startfile(path)

                else:
                    window['-MSG BOX-'].update(f'There are still missing information in Pre-DTW. \nPlease modify the Pre-dtw file in the pop-up directory. \nOr you could find the file at: \n<{path}>')
                    os.chdir(cwd_path)

                    if os.path.exists('ordr_batch_upload.csv'):
                        os.remove('ordr_batch_upload.csv')
                    else:
                        print("ordr_batch_upload.csv does not exist")

                    if os.path.exists('rdr1_batch_upload.csv'):
                        os.remove('rdr1_batch_upload.csv')
                    else:
                        print("rdr1_batch_upload does not exist")

                    #wb.sheets['Dashboard'].range('F21').options(index=False).value = 'There are missing informtion in Pre-dtw, \nplease modify and re-click "Create DTW".'

                    #open pre-dtw directory window
                    os.startfile(path)
            except Exception as e:
                print(e)
                window['-MSG BOX-'].update('')
                window['-MSG BOX-'].update(f'{e} \n Please check if the Pre-DTW excel file is properly closed. \n If not, please close it and press "Create DTW" again.')
                pass

        elif event == '-UPDATE SB1-':
            window['-MSG BOX-'].update(f'Be sure to save and close the SB1 Mapping file before you proceed to the next step!')
            os.startfile(fr"C:\Users\{username}\Groupe SEB\Supply Chain Data Automation - Documents\Programs\15 Malaysia PO Automation\02 SAP Business One Mapping")

    window.close()


if __name__=='__main__':
    MY_PO_app()