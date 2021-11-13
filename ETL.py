import pandas as pd
import sqlalchemy
import numpy as np
import re
from fuzzywuzzy.fuzz import ratio
from tqdm.notebook import tqdm

#NK = pd.read_excel("./5402-NK-T7.2020.xlsx")
def clean(file):
    NK = pd.read_excel(file)
    DS_NK = NK[['Số tờ khai', 'Ngày đăng ký', 'Phương thức vận chuyển', 'Mã người nhập khẩu', 'Tên người nhập khẩu',
           'Địa chỉ người nhập khẩu','Tên người xuất khẩu', 'Địa chỉ 1(Street and number/P.O.BOX)',
            'Địa chỉ 2(Street and number/P.O.BOX)', 'Địa chỉ 3(City name)',
            'Địa chỉ 4(Country sub-entity, name)', 'Mã nước(Country, coded)', 'Tổng trọng lượng hàng (Gross)',
            'Mã đơn vị tính trọng lượng (Gross)', 'Tên địa điểm dỡ hàng', 'Tên địa điểm xếp hàng', 'Phương thức thanh toán', 'Mã điều kiện giá hóa đơn',
            'Mã đồng tiền của hóa đơn', 'Đơn giá hóa đơn', 'Mã đồng tiền của đơn giá', 'Đơn vị của đơn giá và số lượng', 'Thuế suất thuế nhập khẩu']]
    def company_code(df):
        df1 = np.asarray(df)
        df1 = df1.astype(str)
        for i in range(len(df1)):
            if len(df1[i]) == 9 or len(df1[i]) == 12:
                df1[i] = str("0"+str(df1[i]))
            else:
                df1[i] = str(df1[i])
        return df1

    DS_NK['MaSoThue']= company_code(DS_NK['Mã người nhập khẩu'])

    def company_name(name):
        def spliter(l: str, delimiters: list = [',', '-', ';', '>', ')', '(']):
            for d in delimiters:
            # Logic của đoạn này là như nào?
                out = l.split(d)
                if len(out) <= 10 and len(out) > 2:
                    return out
            return out
        out = name.apply(spliter)

        def change(s):
            s = re.sub(r"TRÁCH NHIỆM HỮU HẠN", "TNHH", s)
            s = re.sub(r"MÔÊT THANH VIÊN DÊÊT SHANLI VIÊÊT NAM","MTV DỆT SHANLI VIỆT NAM", s)
            s = re.sub(r"CỔ PHẦN", "CP", s)
            s = re.sub(r"CHI NHÁNH", "CN", s)
            s = re.sub(r"MỘT THÀNH VIÊN", "MTV", s)
            s = re.sub(r"CTY", "CÔNG TY", s)
            s = re.sub(r"\(", "", s)
            s = re.sub(r"\)", "", s)
            return s
        out = name.apply(change)
        return out

    name = DS_NK['Tên người nhập khẩu'].str.upper()
    DS_NK['DoanhNghiepXNK']=company_name(name)

    def customer_location(country, address_id,address):
        df_country=country['ma nuoc']
        country_ETL= df_country['TÊN NƯỚC'].str.upper()
        country_ETL=country_ETL.str.strip()
        ma_nuoc= df_country['MÃ NƯỚC']
        address=address.fillna('').astype(str)
        address['combine']=address.agg(' '.join, axis=1)
        address['combine'] = address['combine'].str.replace('NAN', ' ').str.strip()

        c = [None]*8029
        for i in range(len(address_id)):
            if address_id[i] is not np.nan:
                c.insert(i,address_id[i])
            else:
                    if "VIETNAM" in address['combine'][i]:
                        c.insert(i,"VN")
                    elif "TAMILNADU INDIA" in address['combine'][i]:
                        c.insert(i,"IN")
                    elif "BINH DUONG" in address['combine'][i]:
                        c.insert(i,"VN")
                    elif "INDO" in address['combine'][i]:
                        c.insert(i,"ID")
                    else:
                        for j in range(len(country_ETL)):
                            if country_ETL[j] in address['combine'][i]:
                                c.insert(i,ma_nuoc[j]) 
        return pd.DataFrame(c).head(len(address))


    country = pd.read_excel("./Ma nuoc.xlsx", sheet_name=None)
    address_id=DS_NK['Mã nước(Country, coded)']
    address=DS_NK[['Địa chỉ 1(Street and number/P.O.BOX)','Địa chỉ 2(Street and number/P.O.BOX)', \
        'Địa chỉ 3(City name)','Địa chỉ 4(Country sub-entity, name)']]
    DS_NK['TenNuocXuatKhau']= customer_location(country, address_id,address)

    def company_location(location, location_ETL):
        def normalize(s):
            s = re.sub(r"TP", "", s)
            s = re.sub(r"THÀNH PHỐ", "", s)
            s = re.sub(r"HỒ CHÍ MINH", "HCM", s)
            s = re.sub(r"HỒ CHÍ MÍNH", "HCM", s)
            s = re.sub(r"HO CHI MINH", "HCM", s)
            s = re.sub(r"BÌNH THẠCH","HCM", s)
            s = re.sub(r"BÌNH THẠNH","HCM", s)
            s = re.sub(r"BÌNH TÂN","HCM", s)
            s = re.sub(r"CỦ CHI","HCM", s)
            s = re.sub(r"DN", "ĐỒNG NAI", s)
            s = re.sub(r"DONG NAI", "ĐỒNG NAI", s)
            s = re.sub(r"ĐỒNGNAI", "ĐỒNG NAI", s)
            s = re.sub(r"TIEN GIANG", "TIỀN GIANG", s)
            s = re.sub(r"BINH DUONG","BÌNH DƯƠNG", s)
            s = re.sub(r"BINH DƯƠNG", "BÌNH DƯƠNG", s)
            s = re.sub(r"THU DAU MOT", "BÌNH DƯƠNG", s)
            s = re.sub(r"BD","BÌNH DƯƠNG", s)
            s = re.sub(r"BINH PHUOC","BÌNH PHƯỚC", s)
            s = re.sub(r"BP","BÌNH PHƯỚC", s)
            s = re.sub(r"HÒA BÌNH", "HOÀ BÌNH", s)
            s = re.sub(r"NAM DINH", "NAM ĐỊNH", s)
            s = re.sub(r"BÀ RỊA", "BR VT", s)
            s = re.sub(r"QUANG NAM", "QUẢNG NAM", s)
            s = re.sub(r"THANH HOÁ", "THANH HÓA", s)
            s = re.sub(r"VINH PHUC", "VĨNH PHÚC", s)
            s = re.sub(r"HN", "HÀ NỘI", s)
            s = re.sub(r"HAI PHONG", "HẢI PHÒNG", s)
            s = re.sub(r"\-"," ", s)
            s = re.sub(r"\."," ", s)
            s = re.sub(r"\,"," ", s)
            return s
        location_normalize= location.apply(normalize).dropna()
        location_ETL=location_ETL.apply(normalize).str.strip()
        city=[None]*len(location_normalize)
        for i in range(len(location_normalize)):
            for j in range(len(location_ETL)):
                if location_ETL[j] in location_normalize[i]:
                    city.insert(i,location_ETL[j]) 
        return pd.Series(city).head(len(location_normalize))

    diachi= pd.read_excel("./danhba_diachi.xlsx")
    location= (DS_NK['Địa chỉ người nhập khẩu'].str.upper())
    location_ETL= (diachi['Tỉnh/thành phố'].str.upper())
    DS_NK['TinhThanhPho']=company_location(location, location_ETL)

    def money_nomarlize(money_value, money_code):
        for i in range(len(DS_NK)):
            if money_code[i]=='JPY':
                money_value[i]=np.round(money_value[i]/113.25,2)
                money_code[i]="USD"
            elif money_code[i]=='VND':
                money_value[i]=np.round(money_value[i]/22660,2)
                money_code[i]="USD"
            elif money_code[i]=='KRW':
                money_value[i]=np.round(money_value[i]/1175.7,2)
                money_code[i]="USD"
            elif money_code[i]=='HKD':
                money_value[i]=np.round(money_value[i]/7.79,2)
                money_code[i]="USD"
            elif money_code[i]=='EUR':
                money_value[i]=np.round(money_value[i]/0.8629,2)
                money_code[i]="USD"
            elif money_code[i]=='THB':
                money_value[i]=np.round(money_value[i]/32.74,2)
                money_code[i]="USD"
            elif money_code[i]=='CNY':
                money_value[i]=np.round(money_value[i]/6.3927,2)
                money_code[i]="USD"
        return  money_value

    money_value= DS_NK['Đơn giá hóa đơn']
    money_code= DS_NK['Mã đồng tiền của hóa đơn']
    DS_NK['DonGiaUSD']=money_nomarlize(money_value, money_code)

    def date_time(date):
        datetime=pd.to_datetime(date)
        return datetime

    date= DS_NK['Ngày đăng ký']
    DS_NK['Date']= date_time(date)

    DS_NK = DS_NK[['Số tờ khai', 'Date', 'Phương thức vận chuyển', 'MaSoThue', 'DoanhNghiepXNK',
           'TinhThanhPho','Tên người xuất khẩu','TenNuocXuatKhau', 'Tổng trọng lượng hàng (Gross)',
            'Mã đơn vị tính trọng lượng (Gross)', 'Tên địa điểm dỡ hàng', 'Tên địa điểm xếp hàng', 'Phương thức thanh toán', 'Mã điều kiện giá hóa đơn',
            'Mã đồng tiền của hóa đơn', 'DonGiaUSD', 'Đơn vị của đơn giá và số lượng', 'Thuế suất thuế nhập khẩu']]
    
    
    return DS_NK






'''
# Sua ma doanh nghiep
def company_code(df):
    df1 = np.asarray(df)
    df1 = df1.astype(str)
    for i in range(len(df1)):
        if len(df1[i]) == 9 or len(df1[i]) == 12:
            df1[i] = str("0"+str(df1[i]))
        else:
            df1[i] = str(df1[i])
    return df1

DS_NK['MaSoThue']= company_code(DS_NK['Mã người nhập khẩu'])


#Sưa ten doanh nghiep
def company_name(name):
    def spliter(l: str, delimiters: list = [',', '-', ';', '>', ')', '(']):
        for d in delimiters:
        # Logic của đoạn này là như nào?
             out = l.split(d)
             if len(out) <= 10 and len(out) > 2:
                 return out
        return out
    out = name.apply(spliter)

    def change(s):
         s = re.sub(r"TRÁCH NHIỆM HỮU HẠN", "TNHH", s)
         s = re.sub(r"MÔÊT THANH VIÊN DÊÊT SHANLI VIÊÊT NAM","MTV DỆT SHANLI VIỆT NAM", s)
         s = re.sub(r"CỔ PHẦN", "CP", s)
         s = re.sub(r"CHI NHÁNH", "CN", s)
         s = re.sub(r"MỘT THÀNH VIÊN", "MTV", s)
         s = re.sub(r"CTY", "CÔNG TY", s)
         s = re.sub(r"\(", "", s)
         s = re.sub(r"\)", "", s)
         return s
    out = name.apply(change)
    return out

name = DS_NK['Tên người nhập khẩu'].str.upper()
DS_NK['DoanhNghiepXNK']=company_name(name)
 

#Chuẩn hoá địa chỉ đối tác

def customer_location(country, address_id,address):
    df_country=country['ma nuoc']
    country_ETL= df_country['TÊN NƯỚC'].str.upper()
    country_ETL=country_ETL.str.strip()
    ma_nuoc= df_country['MÃ NƯỚC']
    address=address.fillna('').astype(str)
    address['combine']=address.agg(' '.join, axis=1)
    address['combine'] = address['combine'].str.replace('NAN', ' ').str.strip()

    c = [None]*8029
    for i in range(len(address_id)):
         if address_id[i] is not np.nan:
             c.insert(i,address_id[i])
         else:
                 if "VIETNAM" in address['combine'][i]:
                     c.insert(i,"VN")
                 elif "TAMILNADU INDIA" in address['combine'][i]:
                     c.insert(i,"IN")
                 elif "BINH DUONG" in address['combine'][i]:
                     c.insert(i,"VN")
                 elif "INDO" in address['combine'][i]:
                     c.insert(i,"ID")
                 else:
                     for j in range(len(country_ETL)):
                         if country_ETL[j] in address['combine'][i]:
                             c.insert(i,ma_nuoc[j]) 
    return pd.DataFrame(c).head(len(address))


country = pd.read_excel("./Ma nuoc.xlsx", sheet_name=None)
address_id=DS_NK['Mã nước(Country, coded)']
address=DS_NK[['Địa chỉ 1(Street and number/P.O.BOX)','Địa chỉ 2(Street and number/P.O.BOX)', \
    'Địa chỉ 3(City name)','Địa chỉ 4(Country sub-entity, name)']]
DS_NK['TenNuocXuatKhau']= customer_location(country, address_id,address)



#Chuẩn hoá địa chỉ doanh nghiệp

def company_location(location, location_ETL):
    def normalize(s):
        s = re.sub(r"TP", "", s)
        s = re.sub(r"THÀNH PHỐ", "", s)
        s = re.sub(r"HỒ CHÍ MINH", "HCM", s)
        s = re.sub(r"HỒ CHÍ MÍNH", "HCM", s)
        s = re.sub(r"HO CHI MINH", "HCM", s)
        s = re.sub(r"BÌNH THẠCH","HCM", s)
        s = re.sub(r"BÌNH THẠNH","HCM", s)
        s = re.sub(r"BÌNH TÂN","HCM", s)
        s = re.sub(r"CỦ CHI","HCM", s)
        s = re.sub(r"DN", "ĐỒNG NAI", s)
        s = re.sub(r"DONG NAI", "ĐỒNG NAI", s)
        s = re.sub(r"ĐỒNGNAI", "ĐỒNG NAI", s)
        s = re.sub(r"TIEN GIANG", "TIỀN GIANG", s)
        s = re.sub(r"BINH DUONG","BÌNH DƯƠNG", s)
        s = re.sub(r"BINH DƯƠNG", "BÌNH DƯƠNG", s)
        s = re.sub(r"THU DAU MOT", "BÌNH DƯƠNG", s)
        s = re.sub(r"BD","BÌNH DƯƠNG", s)
        s = re.sub(r"BINH PHUOC","BÌNH PHƯỚC", s)
        s = re.sub(r"BP","BÌNH PHƯỚC", s)
        s = re.sub(r"HÒA BÌNH", "HOÀ BÌNH", s)
        s = re.sub(r"NAM DINH", "NAM ĐỊNH", s)
        s = re.sub(r"BÀ RỊA", "BR VT", s)
        s = re.sub(r"QUANG NAM", "QUẢNG NAM", s)
        s = re.sub(r"THANH HOÁ", "THANH HÓA", s)
        s = re.sub(r"VINH PHUC", "VĨNH PHÚC", s)
        s = re.sub(r"HN", "HÀ NỘI", s)
        s = re.sub(r"HAI PHONG", "HẢI PHÒNG", s)
        s = re.sub(r"\-"," ", s)
        s = re.sub(r"\."," ", s)
        s = re.sub(r"\,"," ", s)
        return s
    location_normalize= location.apply(normalize).dropna()
    location_ETL=location_ETL.apply(normalize).str.strip()
    city=[None]*len(location_normalize)
    for i in range(len(location_normalize)):
        for j in range(len(location_ETL)):
             if location_ETL[j] in location_normalize[i]:
                 city.insert(i,location_ETL[j]) 
    return pd.Series(city).head(len(location_normalize))

diachi= pd.read_excel("./danhba_diachi.xlsx")
location= (DS_NK['Địa chỉ người nhập khẩu'].str.upper())
location_ETL= (diachi['Tỉnh/thành phố'].str.upper())
DS_NK['TinhThanhPho']=company_location(location, location_ETL)


#Chuẩn hoá tên đối tác

def customer_name(customer):

    def name_normalize(s):
        s = re.sub(r"\/"," ", s)
        s = re.sub(r"\."," ", s)
        s = re.sub(r"\,"," ", s)
        s = re.sub(r"LIMITED","LTM", s)
        s = re.sub(r"COMPANY","CO", s)
        s = re.sub(r"CORPORATION","CORP", s)
        s = re.sub(r"INCORPORATED","INC", s)
        s = re.sub(r"IMPORT AND EXPORT"," IMP & EXP", s)
        s = re.sub(r"AND","&", s)
        s = re.sub(r"INT'L","INTERNATIONAL", s)
        s = re.sub(r"PRIVATE","PVT", s)
        s = re.sub(r"LNC","INC", s)
        s = re.sub(r"HYOSUNG INC CORP","HYOSUNG TNC CORP",s)
        s = re.sub(' +', ' ', s)

        return s
    customer_normalize= customer.apply(name_normalize).str.strip() #tim ra cac doi tac duy nhat sau khi chuan hoa
    customer_unique= np.array(customer_normalize.unique())
    
    def func(x1, x2):
        return ratio(x1, x2)
    vfunc = np.vectorize(func)

    score = np.array([])
    for name in tqdm(customer_unique):
        score = np.append(score, vfunc(customer_unique, name), axis=0)
    
    score = score.reshape(len(customer_unique),len(customer_unique))
    score[score==100] = -1    
    max_rows = np.max(score, axis=1).reshape(-1, 1)
    argmax_rows = np.argmax(score, axis=1).reshape(-1, 1)
    customer_ETL=customer_unique
    
    for idx in range(len(customer_unique)):
        if idx in np.where(max_rows>90)[0]: 
            ref = argmax_rows[idx]
            customer_ETL[ref]=customer_unique[idx]
    
    last_score = np.array([])
    for name in tqdm(customer_normalize):
        last_score = np.append(last_score, vfunc(customer_ETL, name), axis=0)
    
    last_score = last_score.reshape(len(customer_normalize),len(customer_ETL))
    argmax_score = np.argmax(last_score, axis=1).reshape(-1,1)

    return pd.DataFrame(customer_ETL[argmax_score])

customer= DS_NK['Tên người xuất khẩu']
DS_NK['TenDoiTacXNK']=customer_name(customer)



# Chuan hoa don vi tien
def money_nomarlize(money_value, money_code):
    for i in range(len(DS_NK)):
        if money_code[i]=='JPY':
            money_value[i]=np.round(money_value[i]/113.25,2)
            money_code[i]="USD"
        elif money_code[i]=='VND':
            money_value[i]=np.round(money_value[i]/22660,2)
            money_code[i]="USD"
        elif money_code[i]=='KRW':
            money_value[i]=np.round(money_value[i]/1175.7,2)
            money_code[i]="USD"
        elif money_code[i]=='HKD':
            money_value[i]=np.round(money_value[i]/7.79,2)
            money_code[i]="USD"
        elif money_code[i]=='EUR':
            money_value[i]=np.round(money_value[i]/0.8629,2)
            money_code[i]="USD"
        elif money_code[i]=='THB':
            money_value[i]=np.round(money_value[i]/32.74,2)
            money_code[i]="USD"
        elif money_code[i]=='CNY':
            money_value[i]=np.round(money_value[i]/6.3927,2)
            money_code[i]="USD"
    return money_code, money_value

money_value= DS_NK['Đơn giá hóa đơn']
money_code= DS_NK['Mã đồng tiền của hóa đơn']
money_nomarlize(money_value, money_code)
        
    
#Chuẩn hoá thời gian
def date_time(date):
    datetime=pd.to_datetime(date)
    return datetime

date= DS_NK['Ngày đăng ký']
DS_NK['Date']= date_time(date)
'''