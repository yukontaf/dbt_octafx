cube(`uninstalls`, {
    sql: `SELECT * FROM dev_gsokolov.appsflyer_uninstall_events_report`,
  
    measures: {
      count: {
        type: `count`,
        drillMembers: [appsflyerId]
      }
    },
  
    dimensions: {
      appsflyerId: {
        sql: `appsflyer_id`,
        type: `string`,
        primaryKey: true
      }
    }
  });