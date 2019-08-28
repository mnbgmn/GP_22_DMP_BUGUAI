package com.utils

/**
  * 指标方法
  */
object RptUtils {
  // 请求点击数
  def request(requestmode:Int,processnode:Int):List[Double]={
    var original = 0
    var valid = 0
    var advertising = 0

    if(requestmode == 1){
      if(processnode >= 1){
        original = 1
        if(processnode >= 2){
          valid = 1
          if(processnode == 3){
            advertising = 1
          }
        }
      }
    }
    List(original,valid,advertising)
  }

  // 点击数
  def click(requestmode:Int,iseffective:Int):List[Double]={
    var show = 0 //展示
    var click = 0 //点击

    if(iseffective == 1){
      if(requestmode == 2){
        show = 1
      }else if(requestmode == 3){
        show = 1
        click = 1
      }
    }
    List(show,click)
  }

  // 竞价
  def bidding(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double): List[Double] ={
    var bid = 0 //参与
    var succeedbid = 0 //竞价成功
    var consume:Double = 0 //消费
    var cost = 0.0 //成本

    if(iseffective == 1 && isbilling == 1){
      if(isbid == 1){
        bid = 1
      }
      if(iswin == 1){
        if(adorderid != 0){
          succeedbid = 1
        }else {
          consume = winprice/1000
          cost = adpayment/1000
        }
      }
    }
    List(bid,succeedbid,consume,cost)
  }
}
