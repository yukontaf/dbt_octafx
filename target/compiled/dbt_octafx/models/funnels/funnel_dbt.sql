




  with event_stream as (  
  select event_nature as event_type, user_id as user_id, timestamp as event_date
  from `analytics-147612`.`dev_gsokolov`.`event_stream`
  where 1 = 1
  
  

    
    
    
   )
  
    , event_stream_step_1 as (
      select event_stream.* 
      from event_stream
      
      where event_stream.event_type = 'communication'
    )

    , step_1 as (
      select count(distinct user_id) as unique_users 
      from event_stream_step_1
    )  

  
    , event_stream_step_2 as (
      select event_stream.* 
      from event_stream
      
        inner join event_stream_step_1 as previous_events
          on event_stream.user_id = previous_events.user_id
          and previous_events.event_type = 'communication'
          and previous_events.event_date <= event_stream.event_date
      
      where event_stream.event_type = 'deal'
    )

    , step_2 as (
      select count(distinct user_id) as unique_users 
      from event_stream_step_2
    )  

  

  , event_funnel as (
    
      select 'communication' as event_type, unique_users, 1 as step_index
      from step_1
      
        union all
      
    
      select 'deal' as event_type, unique_users, 2 as step_index
      from step_2
      
    
  )

  , final as (
    select event_type
      , unique_users, 1.0 * unique_users / nullif(first_value(unique_users) over(order by step_index), 0) as pct_conversion
      , 1.0 * unique_users / nullif(lag(unique_users) over(order by step_index), 0) as pct_of_previous
    from event_funnel
  )

  select * from final
