/*Джоиним следующие таблицы с 2018 по 14 марта 2022 года:
  IA.IA_ASSORT_MATRIX_HISTORY LEFT JOIN
    IA.IA_PRICE_HISTORY LEFT JOIN
        IA.IA_PMIX_SALES_HISTORY
	Формат дат для удобства был переведен в date9. 
	  Некоторые запросы можно оптимизировать, и сначала перевести в date9., а потом
	  производить JOIN, но некоторые таблицы достаточно маленькие и считаются быстро, 
	  поэтому они сделаны через один запрос (full join -> having)*/


%if %sysfunc(sessfound(casauto))=0 %then %do;
cas casauto;
caslib _all_ assign;
%end;

options casDATALIMIT = All;

/* Импортируем, чтобы не считать два нижних запроса*/
proc import file = '/data/sandbox/artem_yukhnevich/DATES_JAN2018_FEB2022.csv'
out = work.DATES_JAN2018_FEB2022
dbms=csv;
run;

proc import file = '/data/sandbox/artem_yukhnevich/ASSORT_MATRIX_HIST_18_22.csv'
out = work.assort_matrix_hist_18_22
dbms=csv;
run;

proc import file = '/data/sandbox/artem_yukhnevich/SKELET_ASSORT_PRHIST_AFTER2018.csv'
out = work.SKELET_ASSORT_PRHIST_AFTER2018
dbms=csv;
run;


proc import file = '/data/sandbox/artem_yukhnevich/PMIX_AFTER2018.csv'
out = work.PMIX_AFTER2018
dbms=csv;
run;

proc import file = '/data/sandbox/artem_yukhnevich/SKELET_AFTER2018.csv'
out = work.SKELET_AFTER2018
dbms=csv;
run;

/*Создадим столбец с датами с 2020 года до февраля 2022*/
proc sql;
CREATE TABLE DATES_JAN2018_FEB2022 as
SELECT distinct SALES_DT as DT
FROM IA.IA_PMIX_SALES_HISTORY
WHERE YEAR(SALES_DT) >= 2018;
quit;

proc sql;
CREATE TABLE DATES_JAN2018_FEB2022 as
SELECT DATEPART(DT) format date9. as DT
FROM DATES_JAN2018_FEB2022
HAVING DT between '01JAN2018'd AND '14MAR2022'd
ORDER BY DT;
quit;


/*Выбираем строки из диапазона 01.01.2018 - 14.03.2022(случаи START > END отсутствуют)*/
proc sql;
CREATE TABLE assort_matrix_hist_18_22 as
SELECT PRODUCT_ID, PBO_LOCATION_ID, 
	   DATEPART(START_DT) format date9. as START_DT, 
	   DT,
	   DATEPART(END_DT) format date9. as END_DT
FROM IA.IA_ASSORT_MATRIX_HISTORY, DATES_JAN2018_FEB2022
HAVING DT BETWEEN START_DT and END_DT
ORDER BY DT;
quit;
/*317.648.647 строк (много из них одинаковых - отличается только DT на 1 день. 
	Например 30 строк для каждого дня с 1 по 30 января для одного товара и ПБО)
	 товаров,  ПБО, 1534 дня*/


/*Приводим таблицу IA_PRICE_HISTORY к виду, с которым удобно работать
   В IA_PRICE_HISTORY мы убрали строки START > END. Так же
	выделили нужный интервал дат.*/
proc sql;
CREATE TABLE PRICE_HIST_AFTER2018 as
SELECT PRODUCT_ID, PBO_LOCATION_ID, GROSS_PRICE_AMT,
	   DATEPART(START_DT) format date9. as START_DT, 
	   DT,
	   DATEPART(END_DT) format date9. as END_DT
FROM IA.IA_PRICE_HISTORY, DATES_JAN2018_FEB2022
WHERE START_DT <= END_DT
HAVING DT BETWEEN START_DT and END_DT;
quit;
/*787.664.014 строк*/


/*К таблице "скелета" слева джоиним цены из IA.IA_PRICE_HISTORY и смотрим покрытие. 
	 (Все ли периоды покрывает таблица цен) */
proc sql;
CREATE TABLE SKELET_ASSORT_PRHIST_AFTER2018 as 
SELECT a.PRODUCT_ID, a.PBO_LOCATION_ID, GROSS_PRICE_AMT, a.DT
FROM assort_matrix_hist_18_22 as a LEFT JOIN PRICE_HIST_AFTER2018 as p
	 ON a.PRODUCT_ID = p.PRODUCT_ID and 
		a.PBO_LOCATION_ID = p.PBO_LOCATION_ID and 
		a.DT = p.DT;
quit;
/*317.648.657 строк*/


/*Создаём таблицу из PMIX с 2018 года, меняя формат времени на date9. */
proc sql;
CREATE TABLE PMIX_AFTER2018 as
SELECT product_id, pbo_location_id, CHANNEL_CD, gross_sales_amt, sales_qty,
	   DATEPART(sales_dt) format date9. as sales_dt
FROM IA.IA_PMIX_SALES_HISTORY
WHERE YEAR(SALES_DT) >= 2018;
quit;
/*Получилось 390.392.549 строк. Сохранили таблицу*/

/*Создаём столбец с ценами для канала ALL. Если Количество проданных товаров равно
нулю, то цене временно присваиваем 0, чтобы потом подтянуть цену из прайсовой матрицы.*/
proc sql;
CREATE TABLE PMIX_CHANNEL_ALL as 
SELECT *, 
	   case when SALES_QTY = 0 then 0
				 else GROSS_SALES_AMT / SALES_QTY end as PMIX_PRICE
FROM PMIX_AFTER2018
WHERE CHANNEL_CD = 'ALL';
quit;
/*Получилось 302.914.775 строк*/


/*Джоиним скелет и PMIX_SALES_HISTORY и находим отсутствие цен в ассортиментной матрице
	среди продаж товаров в прайсовые даты*/
proc sql;
CREATE TABLE skelet_asort_prhist_pmix_aft2018 as
SELECT sk.PRODUCT_ID, sk.PBO_LOCATION_ID, GROSS_PRICE_AMT, DT,
	   GROSS_SALES_AMT, SALES_QTY, PMIX_PRICE
FROM skelet_assort_prhist_after2018 as sk
	 LEFT JOIN PMIX_CHANNEL_ALL as pmix
	 ON sk.PRODUCT_ID = pmix.PRODUCT_ID and sk.PBO_LOCATION_ID = pmix.PBO_LOCATION_ID
	    and DT = SALES_DT;
quit;
/*317.648.657 строк*/

proc sql;
CREATE TABLE SKELET_AFTER2018 as
SELECT PRODUCT_ID, PBO_LOCATION_ID, DT,
	   case when GROSS_PRICE_AMT is null then PMIX_PRICE else GROSS_PRICE_AMT end as 
			GROSS_PRICE_AMT,
	   case when GROSS_SALES_AMT is null then 0 else GROSS_SALES_AMT end as
			GROSS_SALES_AMT, 
	   case when SALES_QTY is null then 0 else SALES_QTY end as 
			SALES_QTY,
	   case when PMIX_PRICE is null or PMIX_PRICE = 0 or 
				 (PMIX_PRICE / GROSS_PRICE_AMT < 1.01 and 
				 PMIX_PRICE / GROSS_PRICE_AMT > 0.99) then GROSS_PRICE_AMT 
				 else PMIX_PRICE end as 
			PMIX_PRICE
FROM skelet_asort_prhist_pmix_aft2018;
quit;
/*317.648.657 строк*/



/*Проверяем долю нулей*/
proc sql;
select count(*)
from SKELET_AFTER2018
where SALES_QTY = 0;
quit;
/*31.478.403 строк или 9.91%*/

proc sql;
select count(*) from SKELET_AFTER2018 where pmix_price is null;
quit;
/*909.313 строк*/
