package entity

import org.apache.ignite.cache.affinity.AffinityKeyMapped

import scala.annotation.meta.field

case class StkOrderKey(TRD_DATE: Int,
                       ORDER_SN: Long,
                       SUBSYS_SN: String) extends Serializable {

}
