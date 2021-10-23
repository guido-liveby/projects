-- $1 - area
-- $2 - start date
-- $3 - start date
select case
    where listdate between $2 and $3 then 1
      else 0
  end average_list_price,
  case
    where closedate between $2 and $3
      and "status" in ('Sold', 'Closed', 'SOLD', 'CLOSED')
      and closeprice != 0 then 1
      else 0
  end average_sales_price,
  case
    where contractdate between $2 and $3 then 1
      else 0
  end contracts_count,
  case
    where closedate between $2 and $3
      and "status" in ('Sold', 'Closed', 'SOLD', 'CLOSED') then 1
      else 0
  end avg_days_on_market,
  case
    where closedate between $2 and $3
      and "status" in ('Sold', 'Closed', 'SOLD', 'CLOSED')
      and closeprice != 0 then 1
      else 0
  end median_sales_price,
  case
    where listdate between $2 and $3 then 1
      else 0
  end new_listings_count,
  case
    where closedate between $2 and $3
      and "status" in ('Sold', 'Closed', 'SOLD', 'CLOSED')
      and (
        closeprice != 0
        and tot_heat_sqft != 0
      ) then 1
      else 0
  end price_per_sqft,
  case
    where closedate between $2 and $3
      and "status" in ('Sold', 'Closed', 'SOLD', 'CLOSED')
      and (
        closeprice != 0
        and listprice != 0
      ) then 1
      else 0
  end sold_to_list_percentage,
  case
    where closedate between $2 and $3
      and "status" in ('Sold', 'Closed', 'SOLD', 'CLOSED') then 1
      else 0
  end total_sales fromproperties
where (
    (
      propertytype in ('Detached', 'Attached', 'Condo')
    )
    and ((area = $))
  );