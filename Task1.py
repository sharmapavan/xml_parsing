#!/usr/bin/python
# -*- coding: utf-8 -*-
from lxml import etree as ET 
import csv
import ftplib
import datetime
import zipfile
import os
import paramiko
from xml.etree import cElementTree
import pandas as pd
import os
import time

csv.register_dialect('escaped', escapechar='\\', doublequote=False, quoting=csv.QUOTE_NONE)
csv.register_dialect('singlequote', quotechar="'", quoting=csv.QUOTE_ALL)


csv_file_store = '/home/pavan/Documents/jsonproject/Project/csv/'  #Save csv path
filename = '/home/pavan/Documents/jsonproject/Json//Extractzipfile/TEMP/'   
filename1 = '/home/pavan/Documents/jsonproject/Json/Extractzipfile/TEMP1/'  
filename2 = '/home/pavan/Documents/jsonproject/Json/Extractzipfile/TEMP2/'  
DownloadZipFile = '/home/pavan/Documents/jsonproject/Json/Dowload1'
Extractfile = '/home/pavan/Documents/jsonproject/Json/Extractzipfile/'


def T_POOL():
	csvfilename = 'OCDCoreData-POOL.csv'
	get_file_name = filename + 'T_POOL.xml'
	list_user = []
	for event,elem in  cElementTree.iterparse(get_file_name):
		try:
			if elem.tag == 'Table':
				member = elem.getchildren()
				pool_idn =  member[0].text
				eff_dte =  member[1].text
				pool_nam =  member[2].text
				DSCV_API_ST_CDE =  member[3].text
				DSCV_API_CNTY_CDE =  member[4].text
				DSCV_API_WELL_IDN =  member[5].text
				STD_SPC_OIL_NUM =  member[6].text
				STD_SPC_GAS_NUM =  member[7].text
				GOR_LIM_NUM =  member[8].text
				TOP_ALLOW_OIL_NUM =  member[9].text
				CSGHD_GAS_LIM_NUM =  member[10].text
				FT_END_LN_NUM =  member[11].text
				FT_SIDE_LN_NUM =  member[12].text
				FT_NEAR_WELL_NUM =  member[13].text
				FT_QQ_LN_NUM =  member[14].text
				ACRE_BASIS_NUM =  member[15].text
				DEL_BASIS_NUM =  member[16].text
				OCD_RULE_ORD_IDN =  member[17].text
				POOL_REG_CDE =  member[18].text
				POOL_TYP_CDE =  member[19].text
				DPTH_ALLOW_MIN_NUM =  member[20].text
				OCD_ORD_TYP_CDE =  member[21].text
				OCD_ORD_NUM =  member[22].text
				OCD_ORD_SFX_IDN =  member[23].text
				SIMULT_DEDT_YON =  member[24].text
				REC_TERMN_DTE =  member[25].text
				list_user.append({
						'pool_idn' : str(pool_idn).strip(),
						'eff_dte' :str(eff_dte).strip(),
						'pool_nam' :str(pool_nam).strip().replace(';', ' ').replace(',', ' '),
						'DSCV_API_ST_CDE' :str(DSCV_API_ST_CDE).strip(),
						'DSCV_API_CNTY_CDE' :str(DSCV_API_CNTY_CDE).strip(),
						'DSCV_API_WELL_IDN' :str(DSCV_API_WELL_IDN).strip(),
						'STD_SPC_OIL_NUM' : str(STD_SPC_OIL_NUM).strip(),
						'STD_SPC_GAS_NUM' : str(STD_SPC_GAS_NUM).strip(),
						'GOR_LIM_NUM' : str(GOR_LIM_NUM).strip(),
						'TOP_ALLOW_OIL_NUM' :str(TOP_ALLOW_OIL_NUM).strip(),
						'CSGHD_GAS_LIM_NUM' :str(CSGHD_GAS_LIM_NUM).strip(),
						'FT_END_LN_NUM' :str(FT_END_LN_NUM).strip(),
						'FT_SIDE_LN_NUM' :str(FT_SIDE_LN_NUM).strip(),
						'FT_NEAR_WELL_NUM' :str(FT_NEAR_WELL_NUM).strip(),
						'FT_QQ_LN_NUM' : str(FT_QQ_LN_NUM).strip(),
						'ACRE_BASIS_NUM' : str(ACRE_BASIS_NUM).strip(),
						'DEL_BASIS_NUM' : str(DEL_BASIS_NUM).strip(),
						'OCD_RULE_ORD_IDN' :str(OCD_RULE_ORD_IDN).strip(),
						'POOL_REG_CDE' :str(POOL_REG_CDE).strip(),
						'POOL_TYP_CDE' :str(POOL_TYP_CDE).strip(),
						'DPTH_ALLOW_MIN_NUM' :str(DPTH_ALLOW_MIN_NUM).strip(),
						'OCD_ORD_TYP_CDE' : str(OCD_ORD_TYP_CDE).strip(),
						'OCD_ORD_NUM' : str(OCD_ORD_NUM).strip(),
						'OCD_ORD_SFX_IDN' : str(OCD_ORD_SFX_IDN).strip(),
						'SIMULT_DEDT_YON' : str(SIMULT_DEDT_YON).strip(),
						'REC_TERMN_DTE' : str(REC_TERMN_DTE).strip()
						})
				elem.clear()
		except:
			pass
			print "**********************************"
	df = pd.DataFrame(list_user, columns =["pool_idn","eff_dte","pool_nam","DSCV_API_ST_CDE","DSCV_API_CNTY_CDE","DSCV_API_WELL_IDN","STD_SPC_OIL_NUM", "STD_SPC_GAS_NUM", "GOR_LIM_NUM","TOP_ALLOW_OIL_NUM","CSGHD_GAS_LIM_NUM", "FT_END_LN_NUM", "FT_SIDE_LN_NUM", "FT_NEAR_WELL_NUM", "FT_QQ_LN_NUM" ,"ACRE_BASIS_NUM", "DEL_BASIS_NUM","OCD_RULE_ORD_IDN","POOL_REG_CDE","POOL_TYP_CDE","DPTH_ALLOW_MIN_NUM","OCD_ORD_TYP_CDE","OCD_ORD_NUM","OCD_ORD_SFX_IDN","SIMULT_DEDT_YON","REC_TERMN_DTE"])
	df.to_csv(csv_file_store + csvfilename)
	sendfileonftpserver(csvfilename)
	# createcsvfile(list_user, writer, csvfilename)
	# list_user = []


def T_WC():
	csvfilename = 'OCDCoreData-WC.csv'
	get_file_name = filename + 'T_WC.xml'
	list_user = []
	for event,elem in  cElementTree.iterparse(get_file_name):
		try:
			if elem.tag == 'Table':
				member = elem.getchildren()
				list_user.append({
						'API_ST_CDE' : member[0].text,
						'API_CNTY_CDE' :member[1].text,
						'API_WELL_IDN' :member[2].text,
						'POOL_IDN' :member[3].text,
						'EFF_DTE' :member[4].text,
						'OGRID_CDE' :member[5].text,
						'WELL_TYP_CDE' :member[6].text,
						'SDIV_TWP_IDN' :member[7].text,
						'SDIV_RNG_IDN' :member[8].text,
						'SDIV_SECT_NUM' :member[9].text,
						'SDIV_UNLT_IDN' :member[10].text,
						'LOT_IDN' :member[11].text,
						'FTG_NS_NUM' :member[12].text,
						'FTG_EW_NUM' :member[13].text,
						'NS_CDE' :member[14].text,
						'EW_CDE' :member[15].text,
						'COMPL_TYP_CDE' :member[16].text,
						'COMPL_DTE' :member[17].text,
						'DPTH_PERF_TOP_NUM' :member[18].text,
						'DPTH_PERF_BTM_NUM' :member[19].text,
						'FST_OIL_PRODN_DTE' :member[20].text,
						'FST_GAS_DELIV_DTE' :member[21].text,
						'TST_DTE' :member[22].text,
						'PRODN_METH_CDE' :member[23].text,
						'TST_LEN_TIM' :member[24].text,
						'TBG_PRES_NUM' :member[25].text,
						'CSG_PRES_NUM' :member[26].text,
						'CHOKE_SIZ_DEC' :member[27].text,
						'TST_OIL_AMT' :member[28].text,
						'TST_WTR_AMT' :member[29].text,
						'TST_GAS_AMT' :member[30].text,
						'API_GRAV_DEC' :member[31].text,
						'TST_METH_CDE' :member[32].text,
						'TST_GOR_DEC' :member[33].text,
						'WC_STAT_CDE' :member[34].text,
						'TST_GAS_DISPN_CDE' :member[35].text,
						'SPC_UNIT_IDN' :member[36].text,
						'PROD_PROP_IDN' :member[37].text,
						'WELL_NBR_IDN' :member[38].text,
						'VENT_APR_CDE' :member[39].text,
						'VENT_APR_DTE' :member[40].text,
						'VENT_APR_EXPR_DTE' :member[41].text,
						'OCD_LEASE_NUM' :member[42].text,
						'WELL_CLASS_CDE' :member[43].text,
						'WELL_GRP_NUM' :member[44].text,
						'BH_PSD_ACT_IND' :member[45].text,
						'SDIV_CONSOL_CDE' :member[46].text,
						'DHC_CMNGL_IND' :member[47].text,
						'OCD_ORD_TYP_CDE' :member[48].text,
						'OCD_ORD_NUM' :member[49].text,
						'OCD_ORD_SFX_IDN' :member[50].text,
						'DHC_DTE' :member[51].text,
						'NFO_PRMT_NUM' :member[52].text,
						'NFO_EXPR_DTE' :member[53].text,
						'DELIV_TST_Q_NUM' :member[54].text,
						'C104_APR_DTE' :member[55].text,
						'GAS_CONN_DTE' :member[56].text,
						'OCD_UNLT_IDN' :member[57].text,
						'REC_TERMN_DTE' :member[58].text,
						})
				elem.clear()
		except:
			pass
			print "**********************************"
	df = pd.DataFrame(list_user, columns =["API_ST_CDE","API_CNTY_CDE","API_WELL_IDN","POOL_IDN","EFF_DTE" ,"OGRID_CDE",
	"WELL_TYP_CDE","SDIV_TWP_IDN","SDIV_RNG_IDN","SDIV_SECT_NUM","SDIV_UNLT_IDN","LOT_IDN","FTG_NS_NUM",
	"FTG_EW_NUM","NS_CDE","EW_CDE","COMPL_TYP_CDE","COMPL_DTE","DPTH_PERF_TOP_NUM","DPTH_PERF_BTM_NUM",
	"FST_OIL_PRODN_DTE","FST_GAS_DELIV_DTE","TST_DTE","PRODN_METH_CDE","TST_LEN_TIM","TBG_PRES_NUM","CSG_PRES_NUM",
	"CHOKE_SIZ_DEC","TST_OIL_AMT","TST_WTR_AMT","TST_GAS_AMT","API_GRAV_DEC","TST_METH_CDE","TST_GOR_DEC","WC_STAT_CDE",
	"TST_GAS_DISPN_CDE","SPC_UNIT_IDN","PROD_PROP_IDN","WELL_NBR_IDN","VENT_APR_CDE","VENT_APR_DTE","VENT_APR_EXPR_DTE",
	"OCD_LEASE_NUM","WELL_CLASS_CDE","WELL_GRP_NUM","BH_PSD_ACT_IND","SDIV_CONSOL_CDE","DHC_CMNGL_IND","OCD_ORD_TYP_CDE","OCD_ORD_NUM","OCD_ORD_SFX_IDN",
	"DHC_DTE","NFO_PRMT_NUM","NFO_EXPR_DTE","DELIV_TST_Q_NUM","C104_APR_DTE","GAS_CONN_DTE","OCD_UNLT_IDN","REC_TERMN_DTE"])
	df.to_csv(csv_file_store + csvfilename, header=True, index=False, encoding='utf-8')
	sendfileonftpserver(csvfilename)
	# createcsvfile(list_user, writer, csvfilename)
	# list_user = []


def T_PROD_PROP():
	csvfilename = 'OCDCoreData-PROD_PROP.csv'
	get_file_name = filename + 'T_PROD_PROP.xml'
	count = 0
	list_user = []
	for event,elem in  cElementTree.iterparse(get_file_name):
		try:
			if elem.tag == 'Table':
				member = elem.getchildren()
				PROD_PROP_IDN = member[0].text
				eff_dte = member[1].text
				prod_prop_nam =member[2].text
				OGRID_CDE = member[3].text
				PROD_PROP_STAT_CDE = member[4].text
				REC_TERMN_DTE = member[5].text
				list_user.append({
					'PROD_PROP_IDN' :PROD_PROP_IDN,
					'eff_dte' :eff_dte,
					'prod_prop_nam': prod_prop_nam,
					'OGRID_CDE' :OGRID_CDE,
					'PROD_PROP_STAT_CDE' :PROD_PROP_STAT_CDE,
					'REC_TERMN_DTE' :REC_TERMN_DTE
					})
				elem.clear()
		except:
			pass
			print "**********************************"
	df = pd.DataFrame(list_user, columns =["PROD_PROP_IDN","eff_dte","prod_prop_nam","OGRID_CDE","PROD_PROP_STAT_CDE" ,"REC_TERMN_DTE"])
	df.to_csv(csv_file_store + csvfilename,header=True, index=False, encoding='utf-8')
	sendfileonftpserver(csvfilename)


def T_PSU():
	csvfilename = 'OCDCoreData-SPU.csv'
	get_file_name = filename + 'T_SPU.xml'
	count = 0
	list_user = []
	for event,elem in  cElementTree.iterparse(get_file_name):
		try:
			if elem.tag == 'Table':
				member = elem.getchildren()
				list_user.append({
					'spc_unit_idn' :member[0].text,
					'eff_dte' :member[1].text,
					'dedt_acre_dec': member[2].text,
					'pnlty_fact_num' :member[3].text,
					'pool_idn' :member[4].text,
					'ocd_ord_typ_cde' :member[5].text,
					'ocd_ord_num' :member[6].text,
					'ocd_ord_sfx_idn' :member[7].text,
					'rec_termn_dte' :member[8].text
					})
				elem.clear()
		except:
			pass
			print "**********************************"
	df = pd.DataFrame(list_user, columns =["spc_unit_idn","eff_dte","dedt_acre_dec","pnlty_fact_num","pool_idn" ,"ocd_ord_typ_cde" ,"ocd_ord_num","ocd_ord_sfx_idn","rec_termn_dte"])
	df.to_csv(csv_file_store + csvfilename,header=True, index=False, encoding='utf-8')
	sendfileonftpserver(csvfilename)


def T_WELL():
	csvfilename = 'OCDCoreData-WELL.csv'
	get_file_name = filename + 'T_WELL.xml'
	count = 0
	list_user = []
	for event,elem in  cElementTree.iterparse(get_file_name):
		try:
			if elem.tag == 'Table':
				member = elem.getchildren()
				list_user.append({
					"API_ST_CDE" : member[0].text,
					"API_CNTY_CDE": member[1].text,
					"API_WELL_IDN": member[2].text,
					"EFF_DTE": member[3].text,
					"WELL_TYP_CDE": member[4].text,
					"OGRID_CDE" : member[5].text,
					"CNTY_CDE" : member[6].text,
					"SDIV_TWP_IDN":  member[7].text,
					"SDIV_RNG_IDN": member[8].text,
					"SDIV_SECT_NUM": member[9].text,
					"SDIV_UNLT_IDN": member[10].text,
					"LOT_IDN": member[11].text,
					"FTG_NS_NUM": member[12].text,
					"FTG_EW_NUM": member[13].text,
					"NS_CDE":member[14].text,
					"EW_CDE": member[15].text,
					"LEASE_TYP_CDE": member[16].text,
					"WELL_STAT_CDE": member[17].text,
					"DPTH_TGT_NUM": member[18].text,
					"ELEV_DF_NUM": member[19].text,
					"ELEV_GL_NUM": member[20].text,
					"ELEV_KT_NUM": member[21].text,
					"SPUD_DTE": member[22].text,
					"DPTH_TOT_NUM": member[23].text,
					"DPTH_PT_NUM": member[24].text,
					"ELEV_CSGHD_NUM": member[25].text,
					"TD_DTE": member[26].text,
					"APD_WRK_TYP_CDE": member[27].text,
					"MULT_SIN_COMPL_IND":member[28].text,
					"CABLE_OR_ROTARY_IN": member[29].text,
					"APR_DTE": member[30].text,
					"PROP_FM_DSC": member[31].text,
					"PROD_PROP_IDN": member[32].text,
					"WELL_NBR_IDN": member[33].text,
					"OCD_LEASE_NUM": member[34].text,
					"DPTH_TVD_NUM": member[35].text,
					"DPTH_MVD_NUM": member[36].text,
					"PLUG_DTE": member[37].text,
					"OCD_UNLT_IDN": member[38].text,
					"REC_TERMN_DTE": member[39].text,
					})
				elem.clear()
		except:
			pass
			print "**********************************"
	df = pd.DataFrame(list_user, columns = ["API_ST_CDE","API_CNTY_CDE","API_WELL_IDN","EFF_DTE","WELL_TYP_CDE" ,"OGRID_CDE" ,
	"CNTY_CDE","SDIV_TWP_IDN","SDIV_RNG_IDN","SDIV_SECT_NUM","SDIV_UNLT_IDN","LOT_IDN","FTG_NS_NUM","FTG_EW_NUM","NS_CDE","EW_CDE","LEASE_TYP_CDE","WELL_STAT_CDE","DPTH_TGT_NUM","ELEV_DF_NUM","ELEV_GL_NUM","ELEV_KT_NUM",
	"SPUD_DTE","DPTH_TOT_NUM","DPTH_PT_NUM","ELEV_CSGHD_NUM","TD_DTE","APD_WRK_TYP_CDE","MULT_SIN_COMPL_IND","CABLE_OR_ROTARY_IN",
	"APR_DTE","PROP_FM_DSC","PROD_PROP_IDN","WELL_NBR_IDN","OCD_LEASE_NUM","DPTH_TVD_NUM","DPTH_MVD_NUM","PLUG_DTE","OCD_UNLT_IDN","REC_TERMN_DTE"])
	df.to_csv(csv_file_store + csvfilename, header=True, index=False, encoding='utf-8')
	sendfileonftpserver(csvfilename)


def T_OGRID():
	csvfilename = '​OCDCoreData-OGRID.csv'
	get_file_name = filename + 'T_OGRID.xml'
	count = 0
	list_user = []
	for event,elem in  cElementTree.iterparse(get_file_name):
		try:
			if elem.tag == 'Table':
				member = elem.getchildren()
				list_user.append({
					"OGRID_CDE" : member[0].text,
					"OGRID_TYP_CDE":member[1].text,
					"OGRID_NAM": member[2].text,
					"OGRID_ADR_NAM": member[3].text,
					"MAIL_STOP": member[4].text,
					"LINE1_ADR" : member[5].text,
					"LINE2_ADR" : member[6].text,
					"LINE3_ADR":  member[7].text,
					"CITY_NAM": member[8].text,
					"ST_NAM": member[9].text,
					"ZIP_CDE": member[10].text,
					"CTRY_NAM": member[11].text,
					"PHONE_NUM": member[12].text,
					"FAX_NUM": member[13].text,
					"OGRID_STAT_CDE": member[14].text,
					"STAT_EFF_DTE": member[15].text,
					"OMID_CDE": member[16].text,
					"ISSNG_AG_CDE": member[17].text,
					"ISSNG_USER": member[18].text,
					"DFA_VEND_CDE":member[19].text,
					"LST_MODIFIED_DTE": member[20].text,
					"OGRID_SRT_NAM": member[21].text,
					"RPT_YON": member[22].text,
					"NOTI_YON": member[23].text,
					"CRT_DTE": member[24].text,
					"EDI_FLNG_MED": member[25].text,
					})
				elem.clear()
		except:
			pass
			print "**********************************"
	df = pd.DataFrame(list_user, columns = ["OGRID_CDE","OGRID_TYP_CDE","OGRID_NAM",
	"OGRID_ADR_NAM","MAIL_STOP" ,"LINE1_ADR" ,"LINE2_ADR","LINE3_ADR","CITY_NAM","ST_NAM",
	"ZIP_CDE","CTRY_NAM","PHONE_NUM","FAX_NUM","OGRID_STAT_CDE",
	"STAT_EFF_DTE","OMID_CDE","ISSNG_AG_CDE","ISSNG_USER",
	"DFA_VEND_CDE","LST_MODIFIED_DTE","OGRID_SRT_NAM",
	"RPT_YON","NOTI_YON","CRT_DTE","EDI_FLNG_MED"])
	df.to_csv(csv_file_store + csvfilename, header=True, index=False, encoding='utf-8')
	sendfileonftpserver(csvfilename)
#end process


#start OCDOtherVolumes20171120
def T_OTHER_DISPN_VOL():
	csvfilename = '​OCDOtherVolumes-T_OTHER_DISPN_VOL.csv'
	get_file_name = filename + 'T_OTHER_DISPN_VOL.xml'
	count = 0
	list_user = []
	for event,elem in  cElementTree.iterparse(get_file_name):
		try:
			if elem.tag == 'Table':
				member = elem.getchildren()
				list_user.append({
					"OGRID_CDE" : member[0].text,
					"POD_IDN":member[1].text,
					"POOL_IDN": member[2].text,
					"PROD_PROP_IDN": member[3].text,
					"SALE_YR_NUM": member[4].text,
					"SALE_MTH_NUM" : member[5].text,
					"PRD_KND_CDE" : member[6].text,
					"DISPN_CDE":  member[7].text,
					"EFF_DTE": member[8].text,
					"DISPN_AMT": member[9].text,
					"FORM_CDE": member[10].text,
					"PLANT_IDN": member[11].text,
					"REC_TERMN_DTE": member[12].text,
					})
				elem.clear()
		except:
			pass
			print "**********************************"
	df = pd.DataFrame(list_user, columns = ["OGRID_CDE","POD_IDN","POOL_IDN","PROD_PROP_IDN","SALE_YR_NUM" ,"SALE_MTH_NUM" ,"PRD_KND_CDE","DISPN_CDE","EFF_DTE","DISPN_AMT",
	"FORM_CDE","PLANT_IDN","REC_TERMN_DTE"])
	df.to_csv(csv_file_store + csvfilename, header=True, index=False, encoding='utf-8')
	sendfileonftpserver(csvfilename)


def T_POD_STOR():
	csvfilename = '​OCDOtherVolumes-T_POD_STOR.csv'
	get_file_name = filename + 'T_POD_STOR.xml'
	count = 0
	list_user = []
	Resident_data = open(csv_file_store + csvfilename, 'wb')
	writer = csv.DictWriter(Resident_data, fieldnames = ["POD_IDN","POOL_IDN","PROD_PROP_IDN","SALE_MTH_NUM","SALE_YR_NUM" ,"OGRID_CDE" ,"EFF_DTE","PRD_KND_CDE","BOM_AMT","EOM_AMT","REC_TERMN_DTE"])
	writer.writeheader()
	csvwriter = csv.writer(Resident_data)
	for event,elem in  cElementTree.iterparse(get_file_name):
		try:
			if elem.tag == 'Table':
				print "*******************",count
				count += 1
				member = elem.getchildren()
				if len(member) == 11:
					list_user.append({
						"POD_IDN" : member[0].text,
						"POOL_IDN": member[1].text,
						"PROD_PROP_IDN":member[2].text,
						"SALE_MTH_NUM": member[3].text,
						"SALE_YR_NUM": member[4].text,
						"OGRID_CDE" : member[5].text,
						"EFF_DTE" : member[6].text,
						"PRD_KND_CDE":  member[7].text,
						"BOM_AMT": member[8].text,
						"EOM_AMT": member[9].text,
						"REC_TERMN_DTE": member[10].text,
						})
				else:
					list_user.append({
						"POD_IDN" : member[0].text,
						"POOL_IDN": member[1].text,
						"PROD_PROP_IDN":member[2].text,
						"SALE_MTH_NUM": member[3].text,
						"SALE_YR_NUM": member[4].text,
						"OGRID_CDE" : member[5].text,
						"EFF_DTE" : member[6].text,
						"PRD_KND_CDE":  member[7].text,
						"BOM_AMT": member[8].text,
						"EOM_AMT": member[9].text,
						"REC_TERMN_DTE": '',
						})
				elem.clear()
		except Exception ,e:
			pass

	createcsvfile(list_user,writer)
	sendfileonftpserver(csvfilename)



def T_POD_VOL():
	# ,'T_POD_VOL_2000_TO_2009.xml'
	filelist_t_pod_Vol = ['T_POD_VOL_1999_AND_PRIOR.xml','T_POD_VOL_2000_TO_2009.xml','T_POD_VOL_2010_AND_LATER.xml']
	csvfilename = ''
	count = 0
	list_user = []
	for filenamne1 in filelist_t_pod_Vol:
		split_file = filenamne1.split('.')
		csvfilename = split_file[1]+'.csv'
		Resident_data = open(csv_file_store + csvfilename, 'wb')
		writer = csv.DictWriter(Resident_data, fieldnames = ["POD_IDN","POOL_IDN","PROD_PROP_IDN","SALE_MTH_NUM","SALE_YR_NUM" ,"PRD_KND_CDE" ,"FROM_OGRID_CDE","TO_OGRID_CDE","EFF_DTE","DISPN_CDE","DISPN_AMT","FORM_CDE","PLANT_IDN","API_GRAVITY","GAS_BTU","REC_TERMN_DTE"])
		writer.writeheader()
		csvwriter = csv.writer(Resident_data)
		get_file_name = filename1 + filenamne1
		for event,elem in  cElementTree.iterparse(get_file_name):
			try:
				if elem.tag == 'Table':
					print "count==============",count
					count += 1
					member = elem.getchildren()
					if len(member) == 16:
						list_user.append({
							"POD_IDN" : member[0].text,
							"POOL_IDN":  member[1].text,
							"PROD_PROP_IDN":  member[2].text,
							"SALE_MTH_NUM":  member[3].text,
							"SALE_YR_NUM": member[4].text,
							"PRD_KND_CDE" :  member[5].text,
							"FROM_OGRID_CDE" :  member[6].text,
							"TO_OGRID_CDE":   member[7].text,
							"EFF_DTE":  member[8].text,
							"DISPN_CDE":  member[9].text,
							"DISPN_AMT":  member[10].text,
							"FORM_CDE":  member[11].text,
							"PLANT_IDN":  member[12].text,
							"API_GRAVITY":  member[13].text,
							"GAS_BTU":  member[14].text,
							"REC_TERMN_DTE":  member[15].text
							})
					elif len(member) == 15:
						list_user.append({
							"POD_IDN" : member[0].text,
							"POOL_IDN":  member[1].text,
							"PROD_PROP_IDN":  member[2].text,
							"SALE_MTH_NUM":  member[3].text,
							"SALE_YR_NUM": member[4].text,
							"PRD_KND_CDE" :  member[5].text,
							"FROM_OGRID_CDE" :  member[6].text,
							"TO_OGRID_CDE":   member[7].text,
							"EFF_DTE":  member[8].text,
							"DISPN_CDE":  member[9].text,
							"DISPN_AMT":  member[10].text,
							"FORM_CDE":  member[11].text,
							"PLANT_IDN":  member[12].text,
							"API_GRAVITY":  member[13].text,
							"GAS_BTU":  member[14].text,
							"REC_TERMN_DTE":  ''
							})
					else:
						list_user.append({
							"POD_IDN" : member[0].text,
							"POOL_IDN":  member[1].text,
							"PROD_PROP_IDN":  member[2].text,
							"SALE_MTH_NUM":  member[3].text,
							"SALE_YR_NUM": member[4].text,
							"PRD_KND_CDE" :  member[5].text,
							"FROM_OGRID_CDE" :  member[6].text,
							"TO_OGRID_CDE":   member[7].text,
							"EFF_DTE":  member[8].text,
							"DISPN_CDE":  member[9].text,
							"DISPN_AMT":  member[10].text,
							"FORM_CDE":  member[11].text,
							"PLANT_IDN":  member[12].text,
							"API_GRAVITY":  member[13].text,
							"GAS_BTU": '',
							"REC_TERMN_DTE": ''
							})
					# df = df.append({"POD_IDN":POD_IDN ,"POOL_IDN":POOL_IDN,"PROD_PROP_IDN" :PROD_PROP_IDN,"SALE_MTH_NUM":SALE_MTH_NUM,"SALE_YR_NUM":SALE_YR_NUM ,"PRD_KND_CDE":PRD_KND_CDE ,"FROM_OGRID_CDE":FROM_OGRID_CDE,"TO_OGRID_CDE":TO_OGRID_CDE,"EFF_DTE":EFF_DTE,"DISPN_CDE":DISPN_CDE,"DISPN_AMT":DISPN_AMT,"FORM_CDE":FORM_CDE,"PLANT_IDN":PLANT_IDN,"API_GRAVITY":API_GRAVITY,"GAS_BTU":GAS_BTU,"REC_TERMN_DTE":REC_TERMN_DTE}, ignore_index=True)
					elem.clear()
			except:
				pass
	print "legth",len(list_user)
	createcsvfile(list_user,writer)
		# df = pd.DataFrame(list_user,columns =["POD_IDN","POOL_IDN","PROD_PROP_IDN","SALE_MTH_NUM","SALE_YR_NUM" ,"PRD_KND_CDE" ,"FROM_OGRID_CDE","TO_OGRID_CDE","EFF_DTE","DISPN_CDE","DISPN_AMT","FORM_CDE","PLANT_IDN","API_GRAVITY","GAS_BTU","REC_TERMN_DTE"])
		# df.to_csv(csv_file_store + csvfilename , header=True, index=False, encoding='utf-8')
		# sendfileonftpserver(csvfilename)
		# path = '.'files_in_dir = [f for f in os.listdir(path) if f.endswith('csv')]


def T_SPC_UNIT_SDIV():
	csvfilename = '​OCDOtherVolumes-T_SPC_UNIT_SDIV.csv'
	get_file_name = filename + 'T_SPC_UNIT_SDIV.xml'
	count = 0
	list_user = []
	for event,elem in  cElementTree.iterparse(get_file_name):
		count += 1
		try:
			if elem.tag == 'Table':
				member = elem.getchildren()
				if len(member) == 11:
					list_user.append({
						"SPC_UNIT_IDN" : member[0].text,
						"SDIV_TWP_IDN": member[1].text,
						"SDIV_RNG_IDN": member[2].text,
						"SDIV_SECT_NUM": member[3].text,
						"SDIV_UNLT_IDN": member[4].text,
						"LOT_IDN" : member[5].text,
						"LOT_TRCT_UNLT_IDN" : member[6].text,
						"CRT_DTE":  member[7].text,
						"OCD_ORD_TYP_CDE": member[8].text,
						"OCD_ORD_NUM": member[9].text,
						"OCD_ORD_SFX_IDN": member[10].text ,
						})
				else:
					list_user.append({
					"SPC_UNIT_IDN" : member[0].text,
					"SDIV_TWP_IDN": member[1].text,
					"SDIV_RNG_IDN": member[2].text,
					"SDIV_SECT_NUM": member[3].text,
					"SDIV_UNLT_IDN": member[4].text,
					"LOT_IDN" : member[5].text,
					"LOT_TRCT_UNLT_IDN" : member[6].text,
					"CRT_DTE":  member[7].text,
					"OCD_ORD_TYP_CDE": member[8].text,
					"OCD_ORD_NUM": member[9].text,
					"OCD_ORD_SFX_IDN": '',
					})
				elem.clear()
		except Exception as e:
			pass
			print ">>>>>>>>>>>>>>>>>",e
	df = pd.DataFrame(list_user, columns = ["SPC_UNIT_IDN","SDIV_TWP_IDN","SDIV_RNG_IDN",
	"SDIV_SECT_NUM","SDIV_UNLT_IDN" ,
	"LOT_IDN" ,"LOT_TRCT_UNLT_IDN","CRT_DTE","OCD_ORD_TYP_CDE","OCD_ORD_NUM",
	"OCD_ORD_SFX_IDN"])
	df.to_csv(csv_file_store + csvfilename, header=True, index=False, encoding='utf-8')
	sendfileonftpserver(csvfilename)


def T_WC_VOL():
	# Make list all filed for T_WC_VOL
	list_all_file = ['T_WC_VOL_1995_AND_PRIOR.xml​', 'T_WC_VOL_1996_TO_1997.xml','T_WC_VOL_1998_TO_1999.xml','T_WC_VOL_2000_TO_2001.xml​','T_WC_VOL_2002_TO_2003.xml','T_WC_VOL_2004_TO_2005.xml','T_WC_VOL_2006_TO_2007.xml','T_WC_VOL_2008_TO_2009.xml','T_WC_VOL_2010_TO_2011.xml','T_WC_VOL_2012_TO_2013.xml​','T_WC_VOL_2014_TO_2015.xml','T_WC_VOL_2016_AND_LATER.xml​']
	csvfilename_list = ['OCDWC-1995_AND_PRIOR.csv', 'OCDWC-1996_TO_1997.csv','OCDWC-1998_TO_1999.csv','OCDWC-2000_TO_2001.csv','OCDWC-2002_TO_2003.csv','OCDWC-2004_TO_2005.csv','OCDWC-2006_TO_2007.csv','OCDWC-2008_TO_2009.csv','OCDWC-2010_TO_2011.csv','OCDWC-2012_TO_2013.csv','OCDWC-2014_TO_2015.csv','OCDWC-2016_AND_LATER.csv']
	test = 0
	# Extract xml file one by one using for loop
	for filenamexml in list_all_file:
		time.sleep(10)
		count = 0
		csvfilename = csvfilename_list[test]
		get_file_name = filename2 + filenamexml
		list_user = []
		test += 1
		for event,elem in  cElementTree.iterparse(get_file_name):
			count +=1 
			if elem.tag == 'Table':
				member = elem.getchildren()
				if len(member) == 18:
					list_user.append({
						"API_ST_CDE" : member[0].text,
						"API_CNTY_CDE": member[1].text,
						"API_WELL_IDN": member[2].text,
						"POOL_IDN": member[3].text,
						"PRODN_MTH": member[4].text,
						"PRODN_YR" : member[5].text,
						"OGRID_CDE" : member[6].text,
						"PRD_KND_CDE":  member[7].text,
						"PROD_INJ_CDE": member[8].text,
						"EFF_DTE": member[9].text,
						"AMEND_IND": member[10].text,
						"C115_WC_STAT_CDE": member[11].text,
						"PROD_AMT": member[12].text,
						"INJ_PRES_NUM": member[13].text,
						"PRODN_DAY_NUM": member[14].text,
						"SALE_AMT": member[15].text,
						"REC_TERMN_DTE": member[16].text,
						"ModifiedDate": member[17].text
						})
				else:
					list_user.append({
						"API_ST_CDE" : member[0].text,
						"API_CNTY_CDE": member[1].text,
						"API_WELL_IDN": member[2].text,
						"POOL_IDN": member[3].text,
						"PRODN_MTH": member[4].text,
						"PRODN_YR" : member[5].text,
						"OGRID_CDE" : member[6].text,
						"PRD_KND_CDE":  member[7].text,
						"PROD_INJ_CDE": member[8].text,
						"EFF_DTE": member[9].text,
						"AMEND_IND": member[10].text,
						"C115_WC_STAT_CDE": member[11].text,
						"PROD_AMT": member[12].text,
						"INJ_PRES_NUM": member[13].text,
						"PRODN_DAY_NUM": member[14].text,
						"SALE_AMT": member[15].text,
						"REC_TERMN_DTE": member[16].text,
						"ModifiedDate": ''
						})
				elem.clear()
		print "count==============",count
		# Convert all list ion csv using pandas
		df = pd.DataFrame(list_user, columns = ["API_ST_CDE","API_CNTY_CDE","API_WELL_IDN",
		"POOL_IDN","PRODN_MTH" ,"PRODN_YR" ,"OGRID_CDE","PRD_KND_CDE","PROD_INJ_CDE","EFF_DTE",
		"AMEND_IND","C115_WC_STAT_CDE","PROD_AMT","INJ_PRES_NUM","PRODN_DAY_NUM","SALE_AMT","REC_TERMN_DTE","ModifiedDate"])
		df.to_csv(csv_file_store + csvfilename, header=True, index=False, encoding='utf-8')
		# Send file on destination server
		sendfileonftpserver(csvfilename)
		list_user = []


def ExtractFolder(zipfile1):
	# extract zip file 
	extract_zip_file = DownloadZipFile + zipfile1
	zip_ref = zipfile.ZipFile(extract_zip_file, 'r')
	# Extract zip file another folder
	zip_ref.extractall(Extractfile)
	

def MergecsvFile():
	# Merge three files in single file(T_POD_VOL)
	CsvFile = ['T_POD_VOL_1999_AND_PRIOR.csv','T_POD_VOL_2000_TO_2009.csv','T_POD_VOL_2010_AND_LATER.csv']
	for filenames in files_in_dir:
		path = csv_file_store + filenames
		df = pd.read_csv(path)
		df.to_csv(CsvFile + 'OCDOtherVolumes-T_POD_VOL.csv', mode='a')
	# Send file on server
	sendfileonftpserver('OCDOtherVolumes-T_POD_VOL.csv')


def createcsvfile(listuser,writer):
	for data in listuser:
		try:
			writer.writerow(data)
		except:
			pass


def ConnectedFtp():

	ftp = ftplib.FTP('100.100.7.7', 'anonymous', 'testing@gmail.com')
	# ftp = ftplib.FTP('164.64.106.6', 'anonymous', 'testing@gmail.com')
	files = ftp.dir()
	# ftp.cwd("/Public/OCD/OCD Data") #changing to /pub/unix
	ftp.cwd("/python_team/pavan")
	ftp.retrlines('LIST')
	date = datetime.datetime.now().strftime("%Y%m%d")
 	
 	# Download all zip file datewise
	list_file = ['OCDCoreData'+str(date)+'.zip' , 'OCDOtherVolumes'+str(date)+'.zip', 'OCDWCVolumes' +str(date)+'.zip']
	for filename in list_file:
		try:
			filename = filename
			gFile = open(filename, "wb")
			ftp.retrbinary('RETR '+ filename, open('/home/pavan/Jasonwork_flow/jsonscrape/downloadzip/'+ filename, 'wb').write)
		except:
			print "No file Found in this directory."	
		ExtractFolder(filename)
	callfunction()


def sendfileonftpserver(filenametest):
	paramiko.util.log_to_file("filename.log")
	client = paramiko.SSHClient()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	# Connect Ftp
	Pem_File_path = '/home/pavan/Documents/jsonproject/Json/pemfile/rajat.pem'
	client.connect('os.uploads.ing.enigma.com', username='psharma', key_filename=Pem_File_path)
	sftp = client.open_sftp()
	sftp.put(csv_file_store + filenametest, 'uploads/'+ filenametest, callback=None, confirm=True)
	

def callfunction():
	funcation = [T_POOL(),T_WC(),T_PROD_PROP(),T_PSU(),T_WELL(),T_OGRID(),T_OTHER_DISPN_VOL(),T_POD_STOR(),T_POD_VOL(),T_SPC_UNIT_SDIV(),T_WC_VOL()]
	try:
		for callfun in funcation:
			time.sleep(10)
			callfun
	except:
		pass

ConnectedFtp()
