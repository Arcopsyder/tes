package com.util

import java.text.SimpleDateFormat

/**
  * 工具类
  */
object Utils_Time {

  //时间工具类
  def consttiem(startTime:String,stopTime:String):Long={
    val df = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    // 20170412030013393282687799171031
    // 开始时间
    val st: Long = df.parse(startTime.substring(0,17)).getTime
    // 结束时间
    val et = df.parse(stopTime).getTime
    et-st
  }

}
