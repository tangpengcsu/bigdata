package org.apache.ignite.spark

class RjbBean extends Serializable {

  var item_le000:String=""
  var asset_d003:String=""
  var asset_l001:String = ""
  var incomepay:String = ""
  var item_de002:String = ""

  override def toString: String = s"${item_le000}-${asset_l001}-${asset_d003}-${incomepay}-${item_de002}"
}
