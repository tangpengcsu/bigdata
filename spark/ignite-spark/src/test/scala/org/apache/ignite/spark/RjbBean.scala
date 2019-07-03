package org.apache.ignite.spark

class RjbBean extends Serializable {

  var item_le000:String=""
  var asset_d003:BigDecimal=BigDecimal(0,2)
  var asset_l001:BigDecimal=BigDecimal(0,2)
  var incomepay:BigDecimal=BigDecimal(0,2)
  var item_de002:String = ""

  override def toString: String = s"${item_le000}-${asset_l001}-${asset_d003}-${incomepay}-${item_de002}"
}
