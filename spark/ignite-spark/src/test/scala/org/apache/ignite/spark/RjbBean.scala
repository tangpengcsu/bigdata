package org.apache.ignite.spark

import com.linuxense.javadbf.spark.DBFFieldProp

class RjbBean extends Serializable {

  @DBFFieldProp("item_le000")
  var item_le000:String=""
  @DBFFieldProp("asset_d003")
  var ASSET_D003:BigDecimal=BigDecimal(0,2)
  @DBFFieldProp("asset_l001")
  var asset_l001:BigDecimal=BigDecimal(0,2)
  @DBFFieldProp("incomepay")
  var incomepay:BigDecimal=BigDecimal(0,2)
  @DBFFieldProp("item_de002")
  var ITEM_DE002:String = ""

  override def toString: String = s"${item_le000}-${asset_l001}-${ASSET_D003}-${incomepay}-${ITEM_DE002}"
}
