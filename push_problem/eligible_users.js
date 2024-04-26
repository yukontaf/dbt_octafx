cube(`eligible_users`, {
    sql: `
  with u as (
      select distinct user_id
      from wh_raw.users
      where EXTRACT(year from registered_dt) = 2024
  ),
  
  b as (
      select distinct SAFE_CAST(user_id as int64) as user_id
      from bloomreach_raw.campaign where
          DATE(timestamp) between '2024-01-01' and DATE(CURRENT_TIMESTAMP)
          and SAFE_CAST(user_id as int64) in (select user_id from u)
          and properties.action_type = 'mobile notification'
          and properties.status = 'delivered'
  )
      SELECT * FROM b
    `,
  
    joins: {
      uninstalls: {
        relationship: `one_to_many`,
        sql: `${CUBE}.user_id = ${uninstalls}.user_id`
      },
  },
  
    measures: {
      count: {
        type: `count`,
        drillMembers: [userId]
      },
    count_distinct: {
      type: `count_distinct`,
      sql: `user_id`
      }
    },
  
    dimensions: {
      userId: {
        sql: `user_id`,
        type: `string`,
        primaryKey: true
      }
    }
  });