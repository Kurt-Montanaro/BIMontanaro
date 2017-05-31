/*
	MCAST IICT
	IICT6011 - Business Intelligence & Reporting
	Assignment 2
*/

USE [iict6011a02];
GO

drop procedure [cubeOrderItem].[sp_registerClient];
go
;
drop table [cubeOrderItem].[product];
drop table [cubeOrderItem].[store];
drop table [cubeOrderItem].[time];
drop table [cubeOrderItem].[client];
drop table [cubeOrderItem].[orderItemFact];
go

drop schema [cubeOrderItem];
go
select * from [oltp].[orderItem]
--Data Cleaning CTE

WITH CTE AS (

SELECT

     productId

    ,orderId

    ,ROW_NUMBER() OVER (PARTITION BY orderId ORDER BY orderId) AS rn

FROM oltp.orderItem)

DELETE FROM CTE WHERE rn > 1

--END CTE , Check for dup data

SELECT productId,orderId , COUNT(*)
FROM oltp.orderItem
GROUP BY productId,orderId
HAVING count(*)>1;

-- Creation
create schema [cubeOrderItem];
go


create table [cubeOrderItem].[time] (
	dateKey uniqueidentifier constraint time_dim_key primary key
		default newsequentialid()
	, yearValue numeric(4,0) not null
	, quarterValue numeric(1,0) not null
	, monthValue numeric(2,0) not null
	, monthDayValue numeric(2,0) not null
	, weekDayValue numeric(1,0) not null
	, dateValue dateTime constraint tim_val_un unique not null
);
go
create table [cubeOrderItem].[client] (
	clientKey uniqueidentifier constraint client_dim_key primary key
		default newsequentialid()
	, country nvarchar(256) not null
	, region nvarchar(256) not null
	, city nvarchar(256) not null
	, postcode integer not null
	, incomeDesc nvarchar(256) not null
	, educationLevel nvarchar(256) not null
	, membershipLevel nvarchar(256) not null
	, occupation nvarchar(256) not null
	, age numeric(3,0) not null
	, gender char(1) not null
	, martialStatus char(1) not null
	, carCount tinyint not null
	, childCount tinyint not null
	, childrenAtResidence tinyint not null
	, isHomeOwner bit not null
	, fromDate date not null
	, toDate date default null
	, clientDesc nvarchar(max) not null
	, clientId uniqueidentifier not null
	, constraint clt_oltp_id unique (clientId, fromDate)
);
go
create table [cubeOrderItem].[store] (
	storeKey uniqueidentifier constraint store_dim_key primary key
		default newsequentialid()
	, country nvarchar(256) not null
	, region nvarchar(256) not null
	, city nvarchar(256) not null
	, postcode int not null
	, storeType nvarchar(256) not null
	, storeName nvarchar(256) not null
	, storeDesc nvarchar(MAX) not null
	, storeId uniqueidentifier not null
		constraint sto_oltp_id unique
);
go
create table [cubeOrderItem].[product] (
	productKey uniqueidentifier constraint product_dim_key primary key
		default newsequentialid()
	, productFamily nvarchar(256) not null
	, department nvarchar(256) not null
	, category nvarchar(256) not null
	, subcategory nvarchar(256) not null
	, brand nvarchar(256) not null
	, productName nvarchar(256) not null
	, productDesc nvarchar(MAX) not null
	, productId uniqueidentifier not null
		constraint prd_oltp_id unique
);
go
create table [cubeOrderItem].[orderItemFact] (
	orderItemFactKey uniqueidentifier constraint orderItem_fact_key primary key
		default newsequentialid()
	, storePrice float not null
	, storeCost float not null
	, unitsSold tinyint not null
	, dateKey uniqueidentifier not null
		constraint  orderItem_dateKey_fk references [cubeOrderItem].[time] (dateKey)
	, storeKey uniqueidentifier not null
		constraint  orderItem_storeKey_fk references [cubeOrderItem].[store] (storeKey)
	, productKey uniqueidentifier not null
		constraint  orderItem_product_fk references [cubeOrderItem].[product] (productKey)
	, clientKey uniqueidentifier not null
		constraint  orderItem_client_fk references [cubeOrderItem].[client] (clientKey)
	, productId uniqueidentifier not null
	, orderId uniqueidentifier not null
	, constraint orderItem_oltp_id unique (productId, orderId)
);
go


--Client Registration


create procedure [cubeOrderItem].[sp_registerClient]
	(@country nvarchar(256), @region nvarchar(256), @city nvarchar(256), @postcode nvarchar(256), @incomeDesc nvarchar(256) , @educationLevel nvarchar(256)
	, @membershipLevel nvarchar(256) , @occupation nvarchar(256), @age numeric(3,0), @gender char(1) , @martialStatus char(1), @carCount tinyint , @childCount tinyint
	,@childrenAtResidence tinyint,@isHomeOwner bit  ,@orderDate date, @clientId uniqueidentifier)
as
begin
	set nocount on;

	if (not exists 
		(select clientKey from [cubeOrderItem].[client]
		where	country=@country and region=@region and city=@city and
				gender=@gender and age=@age and clientId=@clientId))
	begin
		update [cubeOrderItem].[client]
			set toDate=dateadd(day,-1, @orderDate)
			where clientId=@clientId and toDate is null;

		insert [cubeOrderItem].[client]
			(country, region, city, postcode, incomeDesc,educationLevel,membershipLevel,occupation, age,gender,martialStatus, carCount
			,childCount,childrenAtResidence,isHomeOwner, fromDate, toDate, clientDesc, clientId)
			values (@country, @region, @city,@postcode,@incomeDesc, @educationLevel , @membershipLevel , @occupation,@age, @gender ,@martialStatus ,@carCount , @childCount
					, @childrenAtResidence , @isHomeOwner , @orderDate, null
					, concat(@country,',', @region,',', @city,',',@postcode,',',@incomeDesc,',', @educationLevel ,',', @membershipLevel ,',', @occupation,',',
						@age,',', @gender ,',',@martialStatus ,',',@carCount ,',', @childCount
					,',', @childrenAtResidence ,',', @isHomeOwner ,',', @orderDate)
					, @clientId);
	end;
end;
go


-- ETL
begin
	set nocount on;



	-- Date 
	insert into [cubeOrderItem].[time]
		(yearValue, quarterValue, monthValue, monthDayValue, weekDayValue
		,dateValue)
		(select	distinct datepart(year, orderDate), datepart(quarter, orderDate)
				, datepart(month, orderDate), datepart(day, orderDate)
				, datepart(weekday, orderDate), cast(orderDate as date)
		from	[oltp].[order]);

	-- Store

	insert into [cubeOrderItem].[store]
		(country, region, city,postcode,storeType, storeName, storeDesc, storeId)
		(select	distinct cnt.countryName, reg.regionName, cty.cityName ,sto.postCode, sty.typeName
				, sto.storeName
				, concat (cnt.countryName, ',', reg.regionName, ',',
					cty.cityName,',', sto.storeName)
				, sto.storeId
		from	[oltp].[store] sto join [oltp].[city] cty 
				on (sto.cityId=cty.cityId)
				join [oltp].[region] reg
				on (cty.regionId=reg.regionId)
				join [oltp].[country] cnt
				on (reg.countryId=cnt.countryId)
				join [oltp].[storeType] sty
				on (sto.storeTypeId = sty.typeId));
				
	-- Product

	insert into [cubeOrderItem].[product]
		(productFamily, department, category, subcategory, brand, productName
		, productDesc, productId)
		(select	prf.familyName, pdp.departmentName
				, prc.categoryName, psc.subcategoryName
				, brd.brandName, prd.productName
				, concat(prf.familyName, ',', pdp.departmentName
				, ',', prc.categoryName, ',', psc.subcategoryName
				, ',', brd.brandName, ',', prd.productName, ',', prd.productId)
				, prd.productId
		from	[oltp].[productFamily] prf join [oltp].[productDepartment] pdp
				on (pdp.familyId=prf.familyId)
				join [oltp].[productCategory] prc
				on (prc.departmentId = pdp.departmentId)
				join [oltp].[productSubcategory] psc
				on (psc.categoryId = prc.categoryId)
				join [oltp].[product] prd
				on (prd.subcategoryId = psc.subcategoryId)
				join [oltp].[brand] brd
				on (prd.brandId = brd.brandId));

	-- Client


	declare db_cursor cursor for
		select	cnt.countryName, reg.regionName
				, cty.cityName, sto.postCode , yri.incomeDescription , edl.levelName , mem.membershipLevelName
				,occ.occupationName 
				, datediff(year, clt.dateOfBirth, ord.orderDate),clt.gender
				, clt.maritialStatus , clt.carCount , clt.childCount , clt.childAtHomeCount , clt.isHomeOwner
				, cast(ord.orderDate as date), clt.clientId
		from	[oltp].[client] clt join [oltp].[city] cty 
				on (clt.cityId=cty.cityId)
				join [oltp].[region] reg
				on (cty.regionId=reg.regionId)
				join [oltp].[country] cnt
				on (reg.countryId=cnt.countryId)
				join [oltp].[order] ord
				on (clt.clientId=ord.clientId)
				join [oltp].[store] sto
				on (ord.storeId = sto.storeId)
				join [oltp].[yearlyIncome] yri
				on (clt.incomeId = yri.incomeId)
				join [oltp].[educationLevel] edl
				on (clt.levelId = edl.levelId )
				join [oltp].[membershipLevel] mem
				on (clt.membershipLevelId = mem.membershipLevelId)
				join [oltp].[occupation] occ
				on (clt.occupationId = occ.occupationId)
		order by ord.orderDate;


	declare @country nvarchar(256);
	declare @region nvarchar(256);
	declare @city nvarchar(256);
	declare @postcode integer;
	declare @incomeDesc nvarchar(256);
	declare @educationLevel nvarchar(256);
	declare @membershipLevel nvarchar(256);
	declare @occupation nvarchar(256);
	declare @age numeric(3,0);
	declare @gender char(1);
	declare @martialStatus char(1);
	declare @carCount tinyint ;
	declare @childCount tinyint ;
	declare @childrenAtResidence tinyint ;
	declare @isHomeOwner bit;
	declare @orderDate date;
	declare @clientId uniqueidentifier;

	open db_cursor
		fetch next from db_cursor into @country, @region, @city,@postcode,@incomeDesc,@educationLevel
			,@membershipLevel,@occupation,@age, @gender, @martialStatus, @carCount, @childCount ,@childrenAtResidence
			,@isHomeOwner,@orderDate,@clientId ;

	while @@fetch_status=0
	begin
		exec [cubeOrderItem].[sp_registerClient] @country, @region, @city,@postcode,@incomeDesc,@educationLevel
			,@membershipLevel,@occupation,@age, @gender, @martialStatus, @carCount, @childCount ,@childrenAtResidence
			,@isHomeOwner,@orderDate, @clientId ;
		
		fetch next from db_cursor into @country, @region, @city,@postcode,@incomeDesc,@educationLevel
			,@membershipLevel,@occupation,@age, @gender, @martialStatus, @carCount, @childCount ,@childrenAtResidence
			,@isHomeOwner,@orderDate, @clientId;
	end;

	close db_cursor;
	deallocate db_cursor;

	-- Order
	insert into [cubeOrderItem].[orderItemFact]
		(storePrice ,storeCost , unitsSold , dateKey , storeKey , productKey , clientKey , productId , orderId)
		(select	ori.storePrice, ori.storeCost, ori.unitsSold
				, (select dateKey from [cubeOrderItem].[time]
					where dateValue=cast(ord.orderDate as date))
				, (select storeKey from [cubeOrderItem].[store]
					where storeId=ord.storeId)
				, (select productKey from [cubeOrderItem].[product]
					where productId=ori.productId)
				, (select clientKey from [cubeOrderItem].[client]
					where clientId=ord.clientId and
						((toDate is null and ord.orderDate >= fromDate) or
						(toDate is not null and ord.orderDate between fromDate and toDate)))
				,prd.productId ,ord.orderId
		from	[oltp].[order] ord join [oltp].[orderItem] ori
				on (ori.orderId = ord.orderId)
				join [oltp].[product] prd
				on (ori.productId=prd.productId));
end;
go

----table cleaning
--truncate table [cubeOrderItem].[time];
--truncate table [cubeOrderItem].[store];
--truncate table [cubeOrderItem].[product];
--truncate table [cubeOrderItem].[client];
--drop table [cubeOrderItem].[orderItemFact];

--Showing data

select * from [cubeOrderItem].[time];
select * from [cubeOrderItem].[store];
select * from [cubeOrderItem].[product];
select * from [cubeOrderItem].[client] order by  fromDate;
select * from [cubeOrderItem].[orderItemFact];

--Pivot - Report 1  - Product Family vs Quarter-Year

with orderData (productFamily,orderId, yearQValue)
as (select prd.productFamily , oif.orderId , concat(tim.yearValue,'-',tim.quarterValue)
from [cubeOrderItem].[orderItemFact] oif
JOIN [cubeOrderItem].[client] clt
on (oif.clientKey = clt.clientKey)
JOIN [cubeOrderItem].[store] sto
on (oif.storeKey = sto.storeKey)
JOIN [cubeOrderItem].[product] prd
on (oif.productKey=prd.productKey)
JOIN [cubeOrderItem].[time] tim
ON (oif.dateKey=tim.dateKey)
WHERE sto.country = 'USA')
select *
from	orderData
		pivot (count(orderId) for  yearQValue in ([1997-1],[1997-2],[1997-3],[1997-4],[1998-1],[1998-2],[1998-3],[1998-4])) as pvt
order by productFamily;

-- Product Reports

SELECT	Count(DISTINCT prd.productFamily) AS 'Families'
		,Count(DISTINCT prd.department) AS 'Departments' 
		,Count(DISTINCT prd.category) AS 'Categories'
		,Count(DISTINCT prd.subcategory) AS 'Subcategories'
		,Count(DISTINCT prd.productId) AS 'Products'
		,Count(DISTINCT prd.brand) AS 'Brands'
from [cubeOrderItem].[product] prd ;

-- Product Family vs Orders

select prd.productFamily , Count(Distinct oif.orderId) AS 'Orders'
from [cubeOrderItem].[orderItemFact] oif
JOIN [cubeOrderItem].[product] prd
on (oif.productKey=prd.productKey)
GROUP BY prd.productFamily
ORDER BY 2 DESC;

-- Product Department vs Orders

select prd.department , Count(Distinct oif.orderId) AS 'Orders'
from [cubeOrderItem].[orderItemFact] oif
JOIN [cubeOrderItem].[product] prd
on (oif.productKey=prd.productKey)
GROUP BY prd.department
ORDER BY 2 DESC;

-- Categories vs order

select prd.category , Count(Distinct oif.orderId) AS 'Orders'
from [cubeOrderItem].[orderItemFact] oif
JOIN [cubeOrderItem].[product] prd
on (oif.productKey=prd.productKey)
GROUP BY prd.category
ORDER BY 2 DESC;

-- subCategory vs order

select prd.subcategory , Count(Distinct oif.orderId) AS 'Orders'
from [cubeOrderItem].[orderItemFact] oif
JOIN [cubeOrderItem].[product] prd
on (oif.productKey=prd.productKey)
GROUP BY prd.subcategory
ORDER BY 2 DESC;

-- brands vs order

select prd.brand , Count(Distinct oif.orderId) AS 'Orders'
from [cubeOrderItem].[orderItemFact] oif
JOIN [cubeOrderItem].[product] prd
on (oif.productKey=prd.productKey)
GROUP BY prd.brand
ORDER BY 2 DESC;


