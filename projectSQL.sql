with rename_colunm as(
select int64_field_0 as ID
,Ng__y as Ngay
,_____a_ch___ as dia_chi
,Qu___n as Quan_Huyen
,Huy___n as Phuong_Xa
,Lo___i_h__nh_nh______ as loai_hinh_nha_o
,Gi___y_t____ph__p_l__ as giay_to_phap_li
,S____t___ng as so_tang
,S____ph__ng_ng___ as so_phong
,Di___n_t__ch as dien_tich
,D__i as dai
,R___ng as rong
,Gi___m2 as gia
 from `vit-lam-data.kaggle.vietnam_housing_dataset_hanoi`
)
, cast_type AS(
select ID
,Ngay
,dia_chi
,Quan_Huyen
,Phuong_Xa
,loai_hinh_nha_o
,giay_to_phap_li
,so_tang
,CASE WHEN so_tang IN ('Nhiều hơn 10','NaN') THEN NULL
ELSE CAST(so_tang as INTEGER) END AS sotang_integer
,so_phong
--các cột số thường để null không được điền gây tính toán sai
,cast(nullif(replace(replace(replace(so_phong,'nhiều hơn 10 phòng',''),' phòng',''),'NaN',''),'') as numeric) as So_Phong_Integer-- replace từ trong ra
,dai
,cast(nullif(replace(dai,' m',''),'NaN')as numeric) as dai_number
,rong
,cast(nullif(replace(rong,' m',''),'NaN')as numeric) as rong_number
,dien_tich
,cast(nullif(replace(replace(dien_tich,' m²',''),'NaN',''),'') as numeric) as dien_tich_cleased-- có thể dùng 1 replace
,gia
,REPLACE(REPLACE(gia,'.',''),',','.')as gia_cleansed
,concat('Bán ', lower(loai_hinh_nha_o),' tại ', Quan_Huyen)--làm giàu dữ liệu bằng hàm nối

 from rename_colunm)
 , remove_unnsable_rows AS
 (
 select * from cast_type
 where 
 id is not null and gia <> 'NaN')--bỏ dữ liệu sai(null)
 
 --clean cột kí tự
 , clean as (
 select *
 ,coalesce(nullif(Quan_Huyen ,'NaN'),'undefined') as Quan_Huyen -- cac cot ki tu cung xu li dua ve 1 dinh dang(xử lí null) sau đó gán 
  ,coalesce(nullif(Phuong_Xa ,'NaN'),'undefined') as Phuong_Xa
  ,coalesce(nullif(loai_hinh_nha_o ,'NaN'),'undefined') as loai_hinh_nha_o-- các cột kí tự null có thể điền
  ,coalesce(nullif(giay_to_phap_li ,'NaN'),'undefined') as giay_to_phap_li
  from remove_unnsable_rows
 )
 ---chuyển đổi sang số theo chuẩn quốc tế(số nguyên/số thập phân là dấu (.), hàng ngàn/hàng triệu sẽ là dấu (,))
 ,converted_gia as (
 select *
    , CASE WHEN gia_cleansed LIKE '% triệu/m²' THEN CAST(REPLACE(gia_cleansed,' triệu/m²','')AS NUMERIC)*1000000---(1trieu)
            WHEN gia_cleansed LIKE '% tỷ/m²' THEN CAST(REPLACE(gia_cleansed,' tỷ/m²','')AS NUMERIC)*1000000000---(x 1ty)
            WHEN gia_cleansed LIKE '% đ/m²/m²' THEN CAST(REPLACE(gia_cleansed,' đ/m²/m²','')AS NUMERIC)
          END--đưa về cùng 1 đơn vị. đưa về đ/m2
          AS gia_vnd_m2_converted
            
from clean
 )   
 -----làm giàu thêm dữ liệu 
 select * 
        , dien_tich_cleased * gia_vnd_m2_converted as gia_vnd
        ,round(dien_tich_cleased * gia_vnd_m2_converted)/1000000 as gia_trieu_vnd
        ,CASE WHEN dien_tich_cleased<= 50 THEN '1/Diện tích nhỏ'
              WHEN dien_tich_cleased<= 100 THEN '1/Diện tích thường'
              WHEN dien_tich_cleased > 100 THEN '1/Diện tích lớn'
              ELSE 'UNDEFINED'
              END
              as phan_nhom_dien_tich
 from converted_gia
 