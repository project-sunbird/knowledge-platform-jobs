package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"eid":"BE_QR_IMAGE_GENERATOR","processId":"e101e2cf-d592-4095-89fe-38acc0b8b276","objectId":"do_2132430402739240961409","dialcodes":[{"data":"https://preprod.ntp.net.in/dial/N3X6Y3","text":"N3X6Y3","id":"2_N3X6Y3"},{"data":"https://preprod.ntp.net.in/dial/U3J1J9","text":"U3J1J9","id":"0_U3J1J9"},{"data":"https://preprod.ntp.net.in/dial/J9Z5R4","text":"J9Z5R4","id":"1_J9Z5R4"}],"storage":{"container":"dial","path":"01269878797503692810/","fileName":"do_2132430402739240961409_english_class_10_english_1616582317028"},"config":{"errorCorrectionLevel":"H","pixelsPerBlock":2,"qrCodeMargin":3,"textFontName":"Verdana","textFontSize":11,"textCharacterSpacing":0.1,"imageFormat":"png","colourModel":"Grayscale","imageBorderSize":1}}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{"eid":"BE_QR_IMAGE_GENERATOR","processId":"d36c130b-d5cb-4bec-9224-734d1d89bb00","objectId":"do_2132491902071767041339","dialcodes":[{"data":"https://preprod.ntp.net.in/dial/V2B5A2","text":"V2B5A2","id":"1_V2B5A2","location":"http://location1.com"},{"data":"https://preprod.ntp.net.in/dial/F6J3E7","text":"F6J3E7","id":"0_F6J3E7","location":"http://location2.com"}],"storage":{"container":"dial","path":"01272777697873100812/","fileName":"do_2132491902071767041339_sanskrit_class_12_home_science_1617332855179"},"config":{"errorCorrectionLevel":"H","pixelsPerBlock":2,"qrCodeMargin":3,"textFontName":"Verdana","textFontSize":11,"textCharacterSpacing":0.1,"imageFormat":"png","colourModel":"Grayscale","imageBorderSize":1}}
      |""".stripMargin

}