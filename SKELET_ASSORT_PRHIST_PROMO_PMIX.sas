/*Джоиним следующие таблицы с 2020 по февраль 2022 года:
  IA.IA_ASSORT_MATRIX_HISTORY LEFT JOIN
    IA.IA_PRICE_HISTORY LEFT JOIN
      IA.IA_PROMO (with product_id and pbo_location_id) LEFT JOIN
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



/*Импортируем таблицу по промо, в которой есть даты позднее октября 2021*/
libname MIvanov "/data/users/MIvanov/";
proc sql;
CREATE TABLE work.promo_until_mar2022 as
SELECT * FROM MIvanov.promo_pbo_product_c;
quit;


/*Создадим столбец с датами с 2020 года до февраля 2022*/
proc sql;
CREATE TABLE DATES_JAN2020_FEB2022 as
SELECT distinct SALES_DT as DT
FROM IA.IA_PMIX_SALES_HISTORY
WHERE YEAR(SALES_DT) >= 2020;

proc sql;
CREATE TABLE DATES_JAN2020_FEB2022 as
SELECT DATEPART(DT) format date9. as DT
FROM DATES_JAN2020_FEB2022
HAVING YEAR(DT) = 2020 or YEAR(DT) = 2021 or YEAR(DT) = 2022 and MONTH(DT) <= 2
ORDER BY DT;



/*Выбираем строки из диапазона 01.01.2020 - 1.03.2022(случаи START > END отсутствуют)*/
proc sql;
CREATE TABLE assort_matrix_hist_20_22 as
SELECT PRODUCT_ID, PBO_LOCATION_ID, 
	   DATEPART(START_DT) format date9. as START_DT, 
	   DT,
	   DATEPART(END_DT) format date9. as END_DT
FROM IA.IA_ASSORT_MATRIX_HISTORY, DATES_JAN2020_FEB2022
HAVING DT BETWEEN START_DT and END_DT
ORDER BY DT
;
/*194.887.196 строк (много из них одинаковых - отличается только DT на 1 день. 
	Например 30 строк для каждого дня с 1 по 30 января для одного товара и ПБО)
	1303 товаров, 866 ПБО, 790 дней*/

/*Добавим периоды закрытия ПБО (START, потому что start=end во всех строках)*/
proc sql;
CREATE TABLE close_pbo as
SELECT PBO_LOCATION_ID, DATEPART(START_DT) format date9. as CLOSE_PBO
FROM IA.IA_PBO_CLOSE_PERIOD
GROUP BY PBO_LOCATION_ID, START_DT;
/*227.936 строк*/

/*Джоиним с ассортиментной матрицей. MAX выбираем чтобы убрать дубли. Во всех 
	строках max = min*/
proc sql;
CREATE TABLE close_pbo2 as
SELECT PRODUCT_ID, a.PBO_LOCATION_ID, DT, 
	   a.START_DT, a.END_DT, MAX(CLOSE_PBO) as CLOSE_PBO
FROM assort_matrix_hist_20_22 a LEFT JOIN close_pbo c
	 ON a.PBO_LOCATION_ID = c.PBO_LOCATION_ID and DT = CLOSE_PBO
GROUP BY product_id, a.PBO_LOCATION_ID, DT, a.START_DT, a.END_DT;

/*Подгружаем даты открытия различных ПБО*/
proc sql;
CREATE TABLE OPEN_DATES as
SELECT PBO_LOCATION_ID, input(PBO_LOC_ATTR_VALUE, ddmmyy10.) format date9. as open_pbo
FROM IA.IA_PBO_LOC_ATTRIBUTES
WHERE PBO_LOC_ATTR_NM = 'OPEN_DATE';
/*974 строки*/

/*Заменяем столбец на нули для открытого пбо и единицы для закрытого*/
proc sql;
CREATE TABLE assort_and_close_and_open_pbo as
SELECT PRODUCT_ID, CLOSE_PBO2.PBO_LOCATION_ID, DT, START_DT, END_DT, OPEN_PBO,
	   case when CLOSE_PBO is null then 0 else 1 end as CLOSE_PBO
FROM CLOSE_PBO2 LEFT JOIN OPEN_DATES 
	 ON CLOSE_PBO2.PBO_LOCATION_ID = OPEN_DATES.PBO_LOCATION_ID;
/*194.887.196 строк*/



/*Приводим таблицу IA_PRICE_HISTORY к виду, с которым удобно работать
   В IA_PRICE_HISTORY 61852 строки START > END, которые мы убрали. Так же
	выделили нужный интервал дат.*/
proc sql;
CREATE TABLE PRICE_HIST as
SELECT PRODUCT_ID, PBO_LOCATION_ID, GROSS_PRICE_AMT,
	   DATEPART(START_DT) format date9. as START_DT, 
	   DT,
	   DATEPART(END_DT) format date9. as END_DT
FROM IA.IA_PRICE_HISTORY, DATES_JAN2020_FEB2022
WHERE START_DT <= END_DT
HAVING DT BETWEEN START_DT and END_DT;
/*648.041.469 строк*/

/*Убираем дубли, выбирая самый широкий диапазон дат:
	например start1,end1 = 1.01 - 20.01
			 start2,end2 = 10.01 - 30.01 - выводим 1.01 - 30.01
Цена одинаковая, выбрали MEAN. Но можно было по ней сгруппировать.*/
proc sql;
CREATE TABLE PRICE_HIST_ND as
SELECT PRODUCT_ID, PBO_LOCATION_ID, MEAN(GROSS_PRICE_AMT) as GROSS_PRICE_AMT, 
	   MIN(START_DT) as START_PRICE_HIST format = date9., DT, 
	   MAX(END_DT) as END_PRICE_HIST format = date9.
FROM PRICE_HIST
GROUP BY PRODUCT_ID, PBO_LOCATION_ID, DT;
/*648.041.459 строк*/

/*Проверка на дублированные строки, если таковые имеются:
proc sql;
create table doubles as
select product_id, pbo_location_id, GROSS_PRICE_AMT, 
	   START_PRICE_HIST, DT, END_PRICE_HIST, count(product_id) as cnt
from PRICE_HIST_ND
group by product_id, pbo_location_id, DT
having cnt > 1;
*/



/*К таблице "скелета" слева джоиним цены из IA.IA_PRICE_HISTORY и смотрим покрытие. 
	 (Все ли периоды покрывает таблица цен) */
proc sql;
CREATE TABLE SKELET_ASSORT_PRHIST as 
SELECT a.PRODUCT_ID, a.PBO_LOCATION_ID, GROSS_PRICE_AMT, a.DT,
	   a.START_DT as START_assort, a.END_DT as END_assort, OPEN_PBO, CLOSE_PBO,
	   START_PRICE_HIST, END_PRICE_HIST
FROM ASSORT_AND_CLOSE_AND_OPEN_PBO as a LEFT JOIN PRICE_HIST_ND as p
	 ON a.PRODUCT_ID = p.PRODUCT_ID and 
		a.PBO_LOCATION_ID = p.PBO_LOCATION_ID and 
		a.DT = p.DT;
/*194.887.196 строк*/

/*Создаем промо таблицу*/
proc sql;
CREATE TABLE PROMO as
SELECT CHANNEL_CD, PRODUCT_ID, PBO_LOCATION_ID, PROMO_ID, 
	   START_DT, DT, END_DT, END_DT - START_DT as DT_DIFF
FROM PROMO_UNTIL_MAR2022, DATES_JAN2020_FEB2022
WHERE DT BETWEEN START_DT and END_DT;
/*80.765.310 строк*/

/*Запишем в столбец PROMO_MAIN тот promo_id, который для данного продукт-пбо-день
	длится дольше. И в promo_additional запишем тот, который меньше.*/
proc sql;
CREATE TABLE PROMO2 as
SELECT PRODUCT_ID, PBO_LOCATION_ID, 
	   MIN(START_DT) as START_PROMO format = date9., 
	   DT,
	   MAX(END_DT) as END_PROMO format = date9.,
	   case when DT_DIFF = MAX(DT_DIFF) then PROMO_ID end as PROMO_MAIN,
	   case when DT_DIFF <> MAX(DT_DIFF) then PROMO_ID end as PROMO_ADDITIONAL
FROM PROMO
WHERE CHANNEL_CD = 'ALL'
GROUP BY PRODUCT_ID, PBO_LOCATION_ID, DT;
/*45.962.011 строк*/

/*Теперь объединим все Продукт-пбо-дата в одну строку, где будет два значения промо. 
Аггрегатор MAX() используем для того, чтобы избавиться от одинаковых промо для разных каналов*/
proc sql;
CREATE TABLE PROMO3 as
SELECT PRODUCT_ID, PBO_LOCATION_ID, START_PROMO, DT, END_PROMO,
	   MAX(PROMO_MAIN) as PROMO_MAIN, MAX(PROMO_ADDITIONAL) as PROMO_ADDITIONAL
FROM PROMO2
GROUP BY PRODUCT_ID, PBO_LOCATION_ID, START_PROMO, DT, END_PROMO;
/*Получилось 33.588.760 строк*/


/*Джоиним скелет и промо и находим отсутствие цен в ассортиментной матрице
	среди промо товаров в даты асортиментной матрицы*/
proc sql;
CREATE TABLE skelet_assort_prhist_promo as
SELECT matr.PRODUCT_ID, matr.PBO_LOCATION_ID, GROSS_PRICE_AMT, matr.DT, 
	   OPEN_PBO, CLOSE_PBO, START_ASSORT, END_ASSORT, 
	   START_PRICE_HIST, END_PRICE_HIST,
	   START_PROMO, END_PROMO,
	   PROMO_MAIN, PROMO_ADDITIONAL
FROM SKELET_ASSORT_PRHIST as matr 
	 LEFT JOIN PROMO3 as promo
	 ON matr.PRODUCT_ID = promo.PRODUCT_ID and 
	    matr.PBO_LOCATION_ID = promo.PBO_LOCATION_ID and
	    matr.DT = promo.DT;
/*Получилось 194.887.196 строк строк*/



/*Проверка на промо, которые отсутствуют в ассортиментной матрице 
	(5 с 2020 года отсутствуют и 245 присутствуют)
proc sql;
create table skelet2 as
select distinct product_id
from promo_all_join
where year(end_dt) >= 2020 and product_id not in (select distinct product_id
						  from skelet_assort_prhist_promo);			  
proc sql;
select p.product_id, product_nm
from skelet3 s inner join ia.ia_product p
	on p.product_id = s.product_id;
*/



/*Создаём таблицу из PMIX с 2020 года, меняя формат времени на date9. */
proc sql;
CREATE TABLE PMIX_AFTER2020 as
SELECT product_id, pbo_location_id, CHANNEL_CD, gross_sales_amt, sales_qty,
	   DATEPART(sales_dt) format date9. as sales_dt
FROM IA.IA_PMIX_SALES_HISTORY
WHERE YEAR(SALES_DT) >= 2020;
/*Получилось 261.005.337 строк. Сохранили таблицу*/

/*Создаём столбец с ценами для канала ALL. Если Количество проданных товаров равно
нулю, то цене временно присваиваем 0, чтобы потом подтянуть цену из прайсовой матрицы.*/
proc sql;
CREATE TABLE PMIX_CHANNEL_ALL as 
SELECT *, 
	   case when SALES_QTY = 0 then 0
				 else GROSS_SALES_AMT / SALES_QTY end as PMIX_PRICE
FROM PMIX_AFTER2020
WHERE CHANNEL_CD = 'ALL';
/*Получилось 188.814.572 строки*/

/*Выбираем всю таблицу, выделяя нулевые строки Продукт-ПБО
	То есть которые не имеют канала ALL*/
proc sql;
CREATE TABLE PMIX_CHANNEL_NOT_ALL as
SELECT pa.product_id, pa.pbo_location_id, pa.CHANNEL_CD, pa.gross_sales_amt, 
	   pa.sales_qty, pa.sales_dt, 
	   case when pa.SALES_QTY = 0 then 0
				 else pa.GROSS_SALES_AMT / pa.SALES_QTY end as PMIX_PRICE,
	   pc.PRODUCT_ID as product_id_ch_not_all, pc.PBO_LOCATION_ID as pbo_ch_not_all
FROM PMIX_AFTER2020 as pa LEFT JOIN PMIX_CHANNEL_ALL as pc
	 ON pa.PRODUCT_ID = pc.PRODUCT_ID and pa.PBO_LOCATION_ID = pc.PBO_LOCATION_ID and
		pa.SALES_DT = pc.SALES_DT
HAVING product_id_ch_not_all is null and pbo_ch_not_all is null;
/*873 строки*/

/*Таблица со всеми рассчитанными ценами:*/
proc sql;
CREATE TABLE PMIX_CH_PRICE as
SELECT product_id, pbo_location_id, SALES_DT, gross_sales_amt, sales_qty, PMIX_PRICE
FROM PMIX_CHANNEL_ALL
	UNION
SELECT product_id, pbo_location_id, SALES_DT, gross_sales_amt, sales_qty, PMIX_PRICE 
FROM PMIX_CHANNEL_NOT_ALL;
/*188.815.445 строк*/

/*Обратно джоинимся к pmix_after2020, чтобы подтянуть продажи с других каналов.
Складываем всю выручку и всё проданное количество, и указываем рассчитанную ранее цену*/
proc sql;
CREATE TABLE PMIX_AFTER2020_WITH_PRICE as 
SELECT p.PRODUCT_ID, p.PBO_LOCATION_ID, p.SALES_DT, p.GROSS_SALES_AMT, p.SALES_QTY, 
	   PMIX_PRICE
FROM PMIX_AFTER2020 p LEFT JOIN PMIX_CH_PRICE al 
	 ON p.PRODUCT_ID = al.PRODUCT_ID and p.PBO_LOCATION_ID = al.PBO_LOCATION_ID and
		p.SALES_DT = al.SALES_DT;
/*261.005.337 строк*/

/*Проверяем PMIX_AFTER2020 на дубликаты и смотрим отличаются ли цены в них
	Строк с SALES_QTY = 0 обнаружено не было
proc sql;
CREATE TABLE DOUBLES as
SELECT PRODUCT_ID, PBO_LOCATION_ID, SALES_DT,
	    SUM(GROSS_SALES_AMT) as SUM_gross,
		SUM(SALES_QTY) as SUM_qty,
		count(product_id) as cnt
FROM PMIX_AFTER2020
GROUP BY PRODUCT_ID, PBO_LOCATION_ID, SALES_DT
HAVING cnt > 1
ORDER BY PRODUCT_ID, PBO_LOCATION_ID, SALES_DT;
/*Получилась 35.002.671 строка
	из них 3.110.173 строки с разными ценами
		2.688 строк, где по 3 дубликата*/

/*Создаём PMIX без дубликатов, в котором будут просуммированы все продажи и оборот*/
proc sql;
CREATE TABLE PMIX_ND as
SELECT PRODUCT_ID, PBO_LOCATION_ID, SALES_DT, 
	   SUM(GROSS_SALES_AMT) as GROSS_SALES_AMT, SUM(SALES_QTY) as SALES_QTY, PMIX_PRICE
FROM PMIX_AFTER2020_WITH_PRICE
GROUP BY PRODUCT_ID, PBO_LOCATION_ID, SALES_DT, PMIX_PRICE;
/*Получилось 188.815.445 строк*/


/*Джоиним скелет и PMIX_SALES_HISTORY и находим отсутствие цен в ассортиментной матрице
	среди продаж товаров в прайсовые даты*/
proc sql;
CREATE TABLE skelet_assort_prhist_promo_pmix as
SELECT matr.PRODUCT_ID, matr.PBO_LOCATION_ID, GROSS_PRICE_AMT, DT, OPEN_PBO, CLOSE_PBO,
	   START_ASSORT, END_ASSORT, 
	   START_PRICE_HIST, END_PRICE_HIST, 
  	   START_PROMO, END_PROMO,
	   PROMO_MAIN, PROMO_ADDITIONAL,
	   GROSS_SALES_AMT, SALES_QTY, PMIX_PRICE
FROM skelet_assort_prhist_promo as matr 
	 LEFT JOIN PMIX_ND as pmix
	 ON matr.PRODUCT_ID = pmix.PRODUCT_ID and matr.PBO_LOCATION_ID = pmix.PBO_LOCATION_ID
	    and DT = SALES_DT
;
/*194.887.196 строк
	Из них 894.543 нулевых для PRICE_HISTORY
		   170.010.567 промо
		   21.800.742 для pmix*/

proc sql;
CREATE TABLE SKELET as
SELECT PRODUCT_ID, PBO_LOCATION_ID, DT, OPEN_PBO, CLOSE_PBO,
	   START_ASSORT, END_ASSORT,
	   START_PRICE_HIST, END_PRICE_HIST, 
	   case when GROSS_PRICE_AMT is null then PMIX_PRICE else GROSS_PRICE_AMT end as 
			GROSS_PRICE_AMT,
	   START_PROMO, END_PROMO,
	   PROMO_MAIN, PROMO_ADDITIONAL,
	   case when GROSS_SALES_AMT is null then 0 else GROSS_SALES_AMT end as
			GROSS_SALES_AMT, 
	   case when SALES_QTY is null then 0 else SALES_QTY end as SALES_QTY,
	   case when PMIX_PRICE is null or PMIX_PRICE = 0 then GROSS_PRICE_AMT 
			else PMIX_PRICE end as PMIX_PRICE
FROM skelet_assort_prhist_promo_pmix;


/*Заменяем значения в столбце pmix_price на gross_price_amt, если отклонение меньше 1%*/
proc sql;
CREATE TABLE SKELET2 as
SELECT PRODUCT_ID, PBO_LOCATION_ID, DT, OPEN_PBO, CLOSE_PBO,
	   START_ASSORT, END_ASSORT,
	   START_PRICE_HIST, END_PRICE_HIST, GROSS_PRICE_AMT, 
	   START_PROMO, END_PROMO, PROMO_MAIN, PROMO_ADDITIONAL,
	   GROSS_SALES_AMT, SALES_QTY, 
	  case when PMIX_PRICE / GROSS_PRICE_AMT < 1.01 and 
				PMIX_PRICE / GROSS_PRICE_AMT > 0.99 
		   then GROSS_PRICE_AMT 
		   else PMIX_PRICE end as PMIX_PRICE
FROM SKELET;


/*Суммарные продажи по всем продуктам в конктретном ПБО в конкретный день*/
proc sql;
CREATE TABLE SUM_SALES_ON_PBO_IN_DAY as
SELECT PBO_LOCATION_ID, CLOSE_PBO, DT, SUM(SALES_QTY) as SUM_SALES
FROM SKELET2
GROUP BY PBO_LOCATION_ID, CLOSE_PBO, DT;
/*598.556 строк*/

/*Посмотрим на продажи в ПБО, которые хотя бы раз были закрыты*/
proc sql;
CREATE TABLE MEAN_SALES_ON_CLOSE_PBO as
SELECT PBO_LOCATION_ID, CLOSE_PBO, MEAN(SUM_SALES) as MEAN_SALES
FROM SUM_SALES_ON_PBO_IN_DAY
WHERE PBO_LOCATION_ID in (SELECT PBO_LOCATION_ID FROM SUM_SALES_ON_PBO_IN_DAY 
						  WHERE CLOSE_PBO = 1)
GROUP BY PBO_LOCATION_ID, CLOSE_PBO
ORDER BY CLOSE_PBO DESC, MEAN_SALES DESC;


proc sql;
CREATE TABLE zzz as
SELECT m.PBO_LOCATION_ID, m.CLOSE_PBO as CLOSE1, m.MEAN_SALES as SALES1,
	   s.CLOSE_PBO as CLOSE2, s.MEAN_SALES as SALES2
FROM MEAN_SALES_ON_CLOSE_PBO m INNER JOIN MEAN_SALES_ON_CLOSE_PBO s
	 ON m.PBO_LOCATION_ID = s.PBO_LOCATION_ID and m.CLOSE_PBO = 0 and s.CLOSE_PBO = 1;

proc sql;
CREATE TABLE zzz2 as
SELECT PBO_LOCATION_ID, SALES1 as OPEN_TIME_SALES, SALES2 as CLOSE_TIME_SALES, 
	   SALES1 / SALES2 as DIFF
FROM zzz
WHERE SALES1 <> 0 and SALES2 <> 0
ORDER BY DIFF DESC;


proc sql;
create table mmm as
select *, gross_price_amt / pmix_price as diff from skelet2
where gross_price_amt <> pmix_price and pmix_price > 1
order by diff desc;


/*Посмотрим на средние продажи на товар по ресторанам*/
proc sql;
CREATE TABLE MEAN_PROD_SALES as
SELECT PRODUCT_ID, PBO_LOCATION_ID, CLOSE_PBO, MIN(SALES_QTY) min_sales, 
	   MEAN(SALES_QTY) as mean_sales, MAX(SALES_QTY) as max_sales
FROM SKELET2
GROUP BY PRODUCT_ID, PBO_LOCATION_ID, CLOSE_PBO;

proc sql;
select * from ia.ia_product where product_id = 2198;

	
/*Выборочно исследуем подобные рестораны*/
proc sql;
CREATE TABLE Q AS
SELECT DT, CLOSE_PBO, SUM(SALES_QTY) as summ FROM SKELET2 
WHERE PBO_LOCATION_ID = 11001
GROUP BY DT, CLOSE_PBO;




	/*   П Р О В Е Р К И   */

/*Проверяем долю нулей*/
proc sql;
select count(*)
from SKELET
where SALES_QTY = 0;
/*22.016.386 строк или 11.3%*/

/*Проверка на продажи до открытия*/
proc sql;
create table q1 as
select *
from SKELET
where DT < OPEN_PBO and SALES_QTY > 0;
/*0 строк*/

/*Проверка на продажи в закрытом ПБО*/
proc sql;
create table q2 as
select *
from SKELET
where CLOSE_PBO = 1 and SALES_QTY > 0;
/*8.498.291 строка*/

/*Проверка на нулевые цены*/
proc sql;
create table q3 as
select *
from SKELET
where GROSS_SALES_AMT = 0 and SALES_QTY <> 0;
/*1.323.612 строк с продажами товаров за 0 рублей*/

proc sql;
create table q as
select * from SKELET where pmix_price is null;
/*128.652 строк*/

/*Проверка на неравные цены в прайсовой и pmix таблицах*/
proc sql;
create table w as
select *
from SKELET
where pmix_price not is null and gross_price_amt not is null and 
			gross_price_amt <> pmix_price;
/*9.574.335 строк*/
