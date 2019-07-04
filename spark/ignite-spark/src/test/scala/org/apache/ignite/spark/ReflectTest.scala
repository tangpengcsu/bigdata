package org.apache.ignite.spark

import com.linuxense.javadbf.spark.{DBFFieldProp, Utils}
import javassist.bytecode.stackmap.TypeTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.annotation.Annotation
import scala.reflect.ClassTag
import scala.reflect.internal.AnnotationInfos
import scala.util.Random

case class Fruits(id: Int, name: Int) {
  def this() = this(0, 0)
}

class ReflectTest extends FunSuite {

  import scala.reflect.runtime.{universe => ru}

  test("runtime reflect") {
    val l = new C;



    val theType = getTypeTag(l)


    val decls = theType.decls
    val terms = decls.filter(i => i.isTerm && i.asTerm.isVal).map(i => i.asTerm)

    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(l)

    terms.foreach(i => {
      val fm = im.reflectField(i)
      print(fm.symbol.name + "-" + fm.get + "-")
      fm.set(Random.nextInt(1000))
      println(fm.get)
    })


    println(terms)
  }
  test("fkdjfkl") {
    val m = ru.runtimeMirror(getClass.getClassLoader)


    val im = m.reflect(new C)
    val fieldX = ru.typeOf[C].decl(ru.TermName("x")).asTerm.accessed.asTerm

    val fmX = im.reflectField(fieldX)
    println(fmX.get)
    fmX.set(3)
    println(fmX.get)
    val fieldY = ru.typeOf[C].decl(ru.TermName("y")).asTerm.accessed.asTerm

    val fmY = im.reflectField(fieldY)
    println(fmY.get)
    fmY.set(4)
    println(fmY.get)

  }


  test("344") {
    val classMirror = ru.runtimeMirror(getClass.getClassLoader)
    classMirror.reflect(Class.forName("org.apache.ignite.spark.C").newInstance())
  }


  test("classmirror") {


    val classMirror = ru.runtimeMirror(getClass.getClassLoader) //获取运行时类镜像
    val classSymbol = classMirror.classSymbol(Class.forName("com.linuxense.javadbf.RjbBean"))
    val reflectClass = classMirror.reflectClass(classSymbol)
    val typeSignature = reflectClass.symbol.typeSignature
    // val ctorC = typeSignature.decl(ru.termNames.CONSTRUCTOR).asMethod
    val cons = typeSignature.decl(ru.termNames.CONSTRUCTOR).filter(i => i.asMethod.paramLists.flatMap(_.iterator).isEmpty).asMethod
    val ctorm = reflectClass.reflectConstructor(cons)

    val instance = ctorm()
    val vVal = typeSignature.decls.filter(i => i.isTerm && i.asTerm.isVar).map(i => i.asTerm)
    val ref = classMirror.reflect(instance)


    val ann = vVal.map(i=>{
      val  optAnn= i.annotations.find(_.tree.tpe=:=ru.typeOf[DBFFieldProp])
      (i,optAnn)

    })

    //vVal.map(i=>i.annotations.)
    vVal.foreach(i => {

     // i.accessed
      val fm = ref.reflectField(i)


      val z = i.typeSignature.typeSymbol.name.getClass

      //println(fm.symbol.name + "-" + fm.get + "-"+fm.symbol.typeSignature.typeSymbol.name.decodedName.toString+"-"+fm.symbol.annotations+"||")
/*      val ann = fm.symbol.annotations
      val f  = fm.symbol.annotations.find(_.tree.tpe=:=ru.typeOf[DBFFieldProp]).get*/





   /*   def getArgAnnotation[T: TypeTag, U: TypeTag](methodName: String, argName: String) =
        ru.typeOf[T].decl(ru.TermName(methodName)).asMethod.paramLists.collect {
          case symbols => symbols.find(_.name == ru.TermName(argName))
        }.headOption.fold(Option[Annotation](null))(_.get.annotations.find(_.tree.tpe =:= ru.typeOf[U]))*/

   def getCustomAnnotationData(tree: ru.Tree) = {
     val ru.Apply(_, ru.Literal(ru.Constant(name: String)) :: Nil) = tree
     new DBFFieldProp(name)
   }


     // val z1= getCustomAnnotationData(f.tree)


   /*   if(i.name.toString.trim!="s"){
        fm.set(Random.nextInt(1000))
      }*/
     // println(z1)


     // println(fm.get)

    })

    println(instance)


  }

  def getNewTypeTag() = {
    /*    val c = Class.forName("org.apache.ignite.spark.C")
        val z = c.newInstance()*/
    val classMirror = ru.runtimeMirror(getClass.getClassLoader) //获取运行时类镜像

    val classTest = classMirror.staticModule("org.apache.ignite.spark.C")
    val methods = classMirror.reflectModule(classTest)
    val objMirror = classMirror.reflect(methods.instance)

  }

  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeOf[T]

  test("spark") {
    val conf = new SparkConf()
      .setAppName("IgniteRDDExample")
      .setMaster("local[2]")
      .set("spark.executor.instances", "2")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    val orderDF = sparkSession.sparkContext.textFile("").map(_.split(" ")).map(x => Fruits(1, 1)).toDF()
  }

  test("fjsfaj") {
    def main(args: Array[String]): Unit = {
      val cim = ClassCreateUtils("def toUps(str:String):String = str.toUpperCase")
      val value = cim.methods("toUps").invoke(cim.instance, "hello")
      println(value) // method1
      println(cim.invoke("World")) // method2
    }
  }
}

class C(var i: Int,  var s:List[String]) {
  def this() = this(-1,null)

   private val x = 2
   val y = 3
  val w:String ="1"
  val h:BigDecimal =  BigDecimal(1)
  val m:java.math.BigDecimal = new java.math.BigDecimal("11")
  val n :Integer=12
  val j:Char = '1'
  val z:Character = '2'
  val v:Long = 1
  val a:java.lang.Long = null
  val b:Float=1
  val b1:java.lang.Float=null
  val b2:Double = 0
  val b3:java.lang.Double = null
  val b5:Short = 1
  val b6:java.lang.Short = null
  override def toString: String = s"x:${x},y:${y},i:${i},s:${s}"
}