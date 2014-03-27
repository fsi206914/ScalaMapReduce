package probSkyline.dataStructure 
import scala.collection.mutable.ListBuffer

class Instance(val objID: Int, val instID: Int, val prob: Double, val dim: Int)extends Serializable{
	val pt: Point = new Point(dim)
	
	def checkDomination(other: Instance) = pt.checkDomination(other.pt)

	def this(onePoint: Point){
		this(0, 0, 0.0, onePoint.dim)
		this.pt.setPoint (onePoint)
	}

	def setPoint(arr: Array[Double]){
		pt.setArrValue(arr)
	}

	def sum = pt.sum

	override def toString = objID.toString + " " + instID.toString + " " + pt.toString +  " " + prob.toString
}


class Item(val objID: Int) extends Serializable{
	
	val instances = ListBuffer[Instance]()
	var min: Point = null
	var max: Point = null

	def addInstance(inst: Instance){
		instances += inst
	}

	def setMin(aMin: Point){
		min = aMin
	}
	
	def setMax(aMax: Point){
		max = aMax
	}

}
