package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"object":{"id":"do_112806963140329472124"},"index":"true","ets":"1591949601174","eventData":{"action":"update-es-index","stage":1, "identifier":"do_112806963140329472124","channel":"in.ekstep","language":["English"],"name":"Resource Content 1","status":"Draft","level1Concept":["Addition"],"level1Name":["Math-Magic"],"textbook_name":["How Many Times?"],"sourceURL":"https://dev.sunbirded.org/play/content/do_112806963140329472124","source":["Sunbird 1"]}}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{"object":{"id":"do_112806963140329472124"},"index":"false","ets":"1591949601174","eventData":{"action":"update-es-index","stage":1,"identifier":"do_112806963140329472124","channel":"in.ekstep","language":["English"],"name":"Resource Content 1","status":"Draft","level1Concept":["Addition"],"level1Name":["Math-Magic"],"textbook_name":["How Many Times?"],"sourceURL":"https://dev.sunbirded.org/play/content/do_112806963140329472124"}}
      |""".stripMargin

  val EVENT_3: String =
    """
      |{"object":{"id":"do_112806963140329472124"},"index":"true","ets":"1591949601174","eventData":{"action":"update-ml-keywords","stage":"2","ml_Keywords":["maths","addition","add"],"ml_contentText":"This is the content text for addition of two numbers."}}
      |""".stripMargin

  val EVENT_4: String =
    """
      |{"object":{"id":"do_112806963140329472124"},"index":"true","ets":"1591949601174","eventData":{"action":"update-ml-contenttextvector","stage":3,"ml_contentTextVector":[[1.1,2,7.4,68]]}}
      |""".stripMargin

  val EVENT_5: String =
    """
      |{"object":{"id":"do_112806963140329472124"},"index":"true","ets":"1591949601174","eventData":{"action":"update-content-rating","stage":4,"metadata":{"me_averageRating":"1","me_total_time_spent_in_app":"2","me_total_time_spent_in_portal":"3","me_total_time_spent_in_desktop":"4","me_total_play_sessions_in_app":"5","me_total_play_sessions_in_portal":"6","me_total_play_sessions_in_desktop":"7"}}}
      |""".stripMargin

}