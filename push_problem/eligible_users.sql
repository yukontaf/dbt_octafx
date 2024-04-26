cube(`eligible_users`, {
    sql: `

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
