cube(`int_pushes`, {
  sql: `
    SELECT 
      *,
      CASE 
        WHEN event_order = 1 THEN timestamp 
        ELSE NULL 
      END AS first_event 
    FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY timestamp
        ) AS event_order 
      FROM dev_gsokolov.int_bloomreach_events_enhanced
      WHERE safe_cast(user_id as int64) IN (
        SELECT user_id 
        FROM ${eligible_users.sql()}
      )
      AND action_type = 'mobile notification'
      AND date(timestamp) BETWEEN '2024-01-01' AND date(current_timestamp)
      AND status IN ('delivered', 'failed')
    )
  `,

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
      type: `number`,
      primaryKey: true,
      public: true
    },
    eventOrder: {
      sql: `event_order`,
      type: `number`
    },
    status_num: {
      sql: `CASE WHEN ${CUBE}.status='delivered' THEN 1 ELSE 0 END`,
      type: `number`
    },
    action_type: {
      sql: `action_type`,
      type: `string`
    },
    timestamp: {
      sql: `timestamp`,
      type: `time`
    }
  }
});