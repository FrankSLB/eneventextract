# -*- coding: utf-8 -*-

import sys
from constants import EVENT_CODE_SCALE
from read_file import read_key_value_file



def resolve_role_encoding(strOrgRoleCode):

    # 国家代码，可作为前三个字符
    countryCodeSet = ["LBN", "CHE",
			"GNQ", "GTM", "HMD", "IOT", "KNA", "LIE", "MAR", "ARE", "ASM", "BFA", "BHS", "CAN", "COD", "CRI", "TUR",
			"UKR", "USA", "VEN", "VNM", "MYT", "AUS", "EST", "GHA", "HND", "KHM", "LVA", "ABW", "BTN", "MSR", "NPL",
			"PRI", "SGP", "TCA", "YEM", "SCG", "FLK", "GUM", "ITA", "KOR", "MDA", "BRB", "BLZ", "MHL", "NCL", "SPM",
			"SLE", "TLS", "VAT", "CMR", "ERI", "IRN", "JOR", "ALB", "BES", "BIH", "GGY", "FSM", "BLM", "ALA", "CUB",
			"CPV", "CUW", "CXR", "CYP", "CZE", "DEU", "DJI", "DNK", "DMA", "DOM", "DZA", "ECU", "EGY", "ESH", "ESP",
			"ETH", "FIN", "FJI", "FRO", "FRA", "GAB", "GBR", "GRD", "GEO", "GUF", "GIB", "GRL", "GMB", "GIN", "GLP",
			"GRC", "SGS", "GNB", "GUY", "HKG", "HRV", "HTI", "HUN", "IDN", "IRL", "ISR", "IMN", "IND", "IRQ", "ISL",
			"JEY", "JAM", "JPN", "KEN", "KGZ", "KIR", "COM", "PRK", "KWT", "CYM", "KAZ", "LAO", "LCA", "LKA", "LBR",
			"LSO", "LTU", "LUX", "LBY", "MCO", "MNE", "AND", "AFG", "ATG", "AIA", "ARM", "AGO", "ATA", "ARG", "AUT",
			"AZE", "BGD", "BEL", "BGR", "BHR", "BDI", "BEN", "BMU", "BRN", "BOL", "BRA", "BVT", "BWA", "BLR", "CCK",
			"CAF", "COG", "CIV", "COK", "CHL", "CHN", "COL", "MAF", "MDG", "MKD", "MLI", "MMR", "MNG", "MAC", "MNP",
			"MTQ", "MRT", "MLT", "MUS", "MDV", "MWI", "MEX", "MYS", "MOZ", "NAM", "NER", "NFK", "NGA", "NIC", "NLD",
			"NOR", "NRU", "NIU", "NZL", "OMN", "PAN", "PER", "PYF", "PNG", "PHL", "PAK", "POL", "PCN", "PSE", "PRT",
			"PLW", "PRY", "QAT", "REU", "ROU", "SRB", "RUS", "RWA", "SAU", "SLB", "SYC", "SDN", "SWE", "SHN", "SVN",
			"SJM", "SVK", "SMR", "SEN", "SOM", "SUR", "SSD", "STP", "SLV", "SXM", "SYR", "SWZ", "TCD", "ATF", "TGO",
			"THA", "TJK", "TKL", "TKM", "TUN", "TON", "TTO", "TUV", "TWN", "TZA", "UGA", "UMI", "URY", "UZB", "VCT",
			"VGB", "VIR", "VUT", "WLF", "WSM", "XKX", "ZAF", "ZMB", "ZWE", "ANT"]

    # 跨国区域，如非洲、地中海、中亚、中东等，可作为前三个字符
    transnationalRegionCodeSet = ["AFR", "ASA", "BLK", "CRB", "CAU", "CFR", "CAS", "CEU", "EIN", "EAF", "EEU", "EUR",
					"LAM", "MEA", "MDT", "NAF", "NMR", "PGS", "SCN", "SAM", "SAS", "SEA", "SAF", "WAF", "WST"]

    # 没有明显国家归属的actor编码，可作为前三个字符
    internationalCodeSet = ["IGO", "IMG", "INT", "MNC", "NGM", "NGO", "UIS"]

    # 宗教编码，可作为前三个字符
    baseReligionCodeSet = ["ATH", "BAH", "BUD", "CHR", "CON", "HIN", "JAN", "JEW", "MOS", "SHN", "SIK",
					"TAO", "ABR", "ADR", "EAR", "INR", "IRR", "ITR", "NRM", "ZRO"]

    # 宗教编码，可作为4~6字符
    secondaryReligionCodeSet = ["MAH",
			"MLN", "MRN", "SYN", "THR", "VAJ", "ANG", "CTH", "DOX", "CPT", "GNO", "JHW", "LDS", "PRO", "AST", "DEN",
			"WLB", "UDX", "HSD", "SFI", "SHI", "DRZ", "ALE", "SUN", "MAY", "WLN", "UFO", "RAC", "PAG", "ADR", "OFF",
			"ATH", "CON", "HIN", "JAN", "JEW", "MOS", "SHN", "SIK", "TAO", "ABR", "EAR", "INR", "IRR", "ITR", "NRM"]

    # 国内角色编码，如警察、政府，4~6字符，也可以有多个角色编码，按照重要程度进行排列
    generalDomesticCodeSet = ["COP", "GOV", "INS", "JUD", "MIL", "OPP", "REB", "SEP", "SPY", "UAF", "AGR", "BUS", "CRM",
					"CVL", "DEV", "EDU", "ELI", "ENV", "HLH", "HRI", "LAB", "LEG", "MED", "REF", "MOD", "RAD"]

    # 有固定编码的国际组织
    knownGroupCodeSet = [ "AFB", "ABD",
			"BCA", "CEM", "CSS", "ATD", "UEM", "ECA", "WAS", "CFA", "IAC", "IAD", "CEM", "NEP", "OAU", "PAP", "SAD",
			"WAD", "WAM", "ACC", "AEU", "ARL", "AMU", "AMF", "GCC", "APE", "ADB", "ASN", "CIS", "COE", "SCE", "EBR",
			"EFT", "EEC", "SAA", "SOT", "AMN", "CPC", "BIS", "CPA", "CWN", "GOE", "GOS", "GSS", "HIP", "HRW", "IAE",
			"ICO", "JUR", "ICJ", "ICC", "ICG", "FID", "CRC", "IGC", "IHF", "ILO", "IMF", "IOM", "WCT", "IPU", "ITP",
			"IDB", "MSF", "NAT", "OAS", "OIC", "NON", "OPC", "XFM", "PRC", "IRC", "RCR", "UNO", "KID", "FAO", "HCH",
			"HCR", "WBK", "WEF", "WFP", "WHO", "WTO"]

    # 国家编码
    # roleCountryCode = None

    # 有固定编码的国际组织
    knownGroupCode = ''

    # 宗教编码，可作为前3个字符 or 4~6字符
    roleReligionCode = dict(RELIGION1CODE='', RELIGION2CODE='')
    #roleReligionCode_list = list()

    # 国内角色编码
    roleTypeCode = dict(TYPE1CODE='', TYPE2CODE='', TYPE3CODE='')
    #roleTypeCode_list = list()

    idx = 0
    while len(strOrgRoleCode[idx:idx+3]) == 3:
        strtempcode = strOrgRoleCode[idx:idx+3]

        if strtempcode in baseReligionCodeSet:
            roleReligionCode['RELIGION1CODE'] = strtempcode
        elif strtempcode in secondaryReligionCodeSet:
            roleReligionCode['RELIGION2CODE'] = strtempcode
        elif strtempcode in generalDomesticCodeSet:
             if roleTypeCode['TYPE1CODE'] == '':
                 roleTypeCode['TYPE1CODE'] = strtempcode
             elif roleTypeCode['TYPE2CODE'] == '':
                 roleTypeCode['TYPE2CODE'] = strtempcode
             elif roleTypeCode['TYPE3CODE'] == '':
                 roleTypeCode['TYPE3CODE'] = strtempcode
        elif strtempcode in knownGroupCodeSet:
            knownGroupCode = strtempcode

        idx = idx+3

    return knownGroupCode,roleReligionCode,roleTypeCode


# eventrootcode           VARCHAR2(10), -- input
# quadclass               VARCHAR2(5),
# goldsteinscale          NUMBER,

def resolve_quadclass(strEventRootCode):
    ''' 事件四大类编码 '''

    strQuadClass = '0'
    if strEventRootCode:
        tmpintcode = int(strEventRootCode)
        if 1 <= tmpintcode <= 5:
            strQuadClass = '1'
        elif 6 <= tmpintcode <= 9:
            strQuadClass = '2'
        elif 10 <= tmpintcode <= 14:
            strQuadClass = '3'
        elif 15 <= tmpintcode <= 20:
            strQuadClass = '4'

    return strQuadClass



def get_goldsteinscale(strEventCode):
    ''' 事件类型得分 '''

    evtClassScore = 0
    if strEventCode:
        codescale_dict = read_key_value_file(EVENT_CODE_SCALE)
        if strEventCode in codescale_dict:
            evtClassScore = float(codescale_dict[strEventCode])

    return evtClassScore
