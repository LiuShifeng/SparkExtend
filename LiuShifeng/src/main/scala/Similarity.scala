/**
 * Created by LiuShifeng on 2015/1/28.
 */
import scala.math._

/**
 * Created by LiuShifeng on 2014/11/13.
 */
object InterestVector {
  //Variable
  val L1 = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 20, 21, 22)
  val L2 = Array(24, 25, 26, 27, 29, 31, 32, 33, 34, 35, 36, 40, 41, 42, 43, 44, 45, 46, 47, 50, 52, 54, 56, 59, 60, 61, 63, 69, 70, 71, 75, 76, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 108, 112, 113, 114, 116, 117, 119, 120, 121, 122, 123, 124, 125, 126, 128, 129, 130, 131, 136, 137, 138, 141, 142, 143, 144, 145, 146, 149, 150, 151, 154, 158, 159, 161, 162, 163, 164, 165)
  val L3 = Array(166, 167, 170, 171, 201, 205, 215, 219, 222, 225, 226, 227, 228, 229, 230, 231, 232, 233, 237, 238, 239, 240, 241, 242, 245, 246, 248, 249, 250, 251, 252, 257, 258, 260, 263, 264, 265, 266, 267, 268, 270, 273, 275, 277, 278, 279, 280, 281, 283, 288, 289, 291, 293, 294, 295, 296, 297, 298, 300, 301, 304, 305, 306, 307, 308, 309, 310, 313, 318, 319, 321, 323, 324, 326, 327, 328, 329, 331, 333, 336, 337, 355, 356, 357, 359, 377, 379, 380, 381, 382, 384, 387, 391, 394, 397, 399, 400, 401, 426, 427, 428, 429, 430, 432, 433, 435, 438, 440, 441, 442, 443, 444, 445, 447, 448, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474)
  val L4 = Array(490, 491, 492, 493, 494, 495, 496, 497, 509, 510, 536, 538, 546, 561, 563, 581, 582, 583, 584, 585, 586, 600, 606, 607, 608, 609, 610, 611, 612, 613, 616, 618, 619, 620, 621, 622, 623, 624, 626, 628, 629, 631, 632, 633, 634, 636, 638, 639)
  val C1 = Array(0, 24, 25, 26, 166, 167, 170, 171, 201, 205)
  val C2 = Array(1, 27, 29, 215, 219, 222, 225, 226, 227, 228)
  val C3 = Array(2, 31, 32, 33, 34, 229, 230, 231, 232, 233, 237, 238, 239)
  val C4 = Array(3, 35, 36, 40)
  val C5 = Array(4, 41, 42, 43, 44, 45, 240, 241, 242, 245, 246, 248, 249, 250, 251, 252, 490, 491, 492, 493, 494, 495, 496, 497)
  val C6 = Array(5, 46, 47, 50, 52, 54, 56, 59, 60, 61, 63, 69, 70, 257, 258, 260)
  val C7 = Array(6, 71, 75, 76, 263, 264, 265, 266, 267, 268, 270, 509, 510)
  val C8 = Array(7, 80, 81, 82, 83, 84, 273, 275)
  val C9 = Array(8, 85, 86, 87, 88, 89, 90, 91, 92, 93, 277, 278, 279, 280, 281, 283, 288, 289, 291, 293, 294, 295)
  val C10 = Array(9, 94, 95, 96, 97, 98, 296)
  val C11 = Array(10, 99, 100, 297, 298, 300, 301, 304, 305, 306, 307, 308, 309, 536, 538)
  val C12 = Array(101, 102, 103, 310, 313)
  val C13 = Array()
  val C14 = Array(13, 108, 318, 319, 321, 323, 324, 326, 327, 328, 329, 331, 333, 336, 337, 546)
  val C15 = Array(14, 112)
  val C16 = Array(15, 113, 114, 116, 117, 119, 120, 121, 122, 123, 124, 125, 126, 128, 355, 356, 357, 359, 377, 379, 380, 381, 382, 384, 387, 391, 394, 397, 399, 400, 401, 426, 427, 428, 429, 430, 432, 433, 435, 438, 440, 441, 442, 443, 444, 445, 447, 561, 563)
  val C17 = Array(16, 129, 130, 131, 448, 450, 451)
  val C18 = Array(17)
  val C19 = Array(18, 136, 137, 138)
  val C20 = Array(452, 453, 454, 455)
  val C21 = Array(20, 141, 142, 143, 144, 145, 146, 456, 457, 458, 459, 460, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474, 581, 582, 583, 584, 585, 586, 600, 606, 607, 608, 609, 610, 611, 612, 613, 616, 618, 619, 620, 621, 622, 623, 624, 626, 628, 629, 631, 632, 633, 634, 636, 638, 639)
  val C22 = Array(21, 149, 150, 151, 154)
  val C23 = Array(22, 158, 159, 161, 162, 163, 164, 165)

  val Ll1 = L1.length
  val Ll2 = L2.length
  val Ll3 = L3.length
  val Ll4 = L4.length
  val Ll = Ll1 + Ll2 + Ll3 + Ll4
  val Lc1 = C1.length
  val Lc2 = C2.length
  val Lc3 = C3.length
  val Lc4 = C4.length
  val Lc5 = C5.length
  val Lc6 = C6.length
  val Lc7 = C7.length
  val Lc8 = C8.length
  val Lc9 = C9.length
  val Lc10 = C10.length
  val Lc11 = C11.length
  val Lc12 = C12.length
  val Lc13 = C13.length
  val Lc14 = C14.length
  val Lc15 = C15.length
  val Lc16 = C16.length
  val Lc17 = C17.length
  val Lc18 = C18.length
  val Lc19 = C19.length
  val Lc20 = C20.length
  val Lc21 = C21.length
  val Lc22 = C22.length
  val Lc23 = C23.length
}
object Similarity {
  /**
   * Entropy
   * @param a Array[Double]
   * @return Double
   */
  def Entropy(a:Array[Double]):Double={
    val sum = a.sum
    val l = a.length
    var result = 0.0
    for(i<-0 until l){
      if(a(i)>0){
        result = result - a(i)/sum*math.log(a(i)/sum)
      }
    }
    result
  }

  /**
   * LevelSim
   * Location-based and Preference-Aware Recommendation Using Sparse Geo-Social Networking Data
   * @param a Arrau[Double]
   * @param b Array[Double
   * @return Double
   */
  def levelSim(a:Array[Double],b:Array[Double]):Double={
    val l = a.length
    var result = 0.0
    for(i<-0 until l){
      result = result + min(a(i),b(i))
    }
    result
  }

  def hierachySim(a:Array[Double],b:Array[Double]):Double={
    val level_a_1 = new Array[Double](24)
    val level_a_2 = new Array[Double](142)
    val level_a_3 = new Array[Double](309)
    val level_a_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_a_1(i) = a(i)
    }
    for(i<-24 until 166){
      level_a_2(i-24) = a(i)
    }
    for(i<-166 until 475){
      level_a_3(i-166) = a(i)
    }
    for(i<-475 until 643){
      level_a_4(i-475) = a(i)
    }

    val level_b_1 = new Array[Double](24)
    val level_b_2 = new Array[Double](142)
    val level_b_3 = new Array[Double](309)
    val level_b_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_b_1(i) = b(i)
    }
    for(i<-24 until 166){
      level_b_2(i-24) = b(i)
    }
    for(i<-166 until 475){
      level_b_3(i-166) = b(i)
    }
    for(i<-475 until 643){
      level_b_4(i-475) = b(i)
    }

    val Sim = math.pow(2,0)*levelSim(level_a_1,level_b_1)/(1+math.max(Entropy(level_a_1),Entropy(level_b_1))-math.min(Entropy(level_a_1),Entropy(level_b_1))) +
      math.pow(2,1)*levelSim(level_a_2,level_b_2)/(1+math.max(Entropy(level_a_2),Entropy(level_b_2))-math.min(Entropy(level_a_2),Entropy(level_b_2))) +
      math.pow(2,2)*levelSim(level_a_3,level_b_3)/(1+math.max(Entropy(level_a_3),Entropy(level_b_3))-math.min(Entropy(level_a_3),Entropy(level_b_3))) +
      math.pow(2,3)*levelSim(level_a_4,level_b_4)/(1+math.max(Entropy(level_a_4),Entropy(level_b_4))-math.min(Entropy(level_a_4),Entropy(level_b_4)))
    Sim
  }

  def hierachySim(a:Array[Double],b:Array[Double],c:Array[Double],d:Array[Double]):Double={
    val level_a_1 = new Array[Double](24)
    val level_a_2 = new Array[Double](142)
    val level_a_3 = new Array[Double](309)
    val level_a_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_a_1(i) = a(i)
    }
    for(i<-24 until 166){
      level_a_2(i-24) = a(i)
    }
    for(i<-166 until 475){
      level_a_3(i-166) = a(i)
    }
    for(i<-475 until 643){
      level_a_4(i-475) = a(i)
    }

    val level_b_1 = new Array[Double](24)
    val level_b_2 = new Array[Double](142)
    val level_b_3 = new Array[Double](309)
    val level_b_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_b_1(i) = b(i)
    }
    for(i<-24 until 166){
      level_b_2(i-24) = b(i)
    }
    for(i<-166 until 475){
      level_b_3(i-166) = b(i)
    }
    for(i<-475 until 643){
      level_b_4(i-475) = b(i)
    }

    val level_c_1 = new Array[Double](24)
    val level_c_2 = new Array[Double](142)
    val level_c_3 = new Array[Double](309)
    val level_c_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_c_1(i) = c(i)
    }
    for(i<-24 until 166){
      level_c_2(i-24) = c(i)
    }
    for(i<-166 until 475){
      level_c_3(i-166) = c(i)
    }
    for(i<-475 until 643){
      level_c_4(i-475) = c(i)
    }

    val level_d_1 = new Array[Double](24)
    val level_d_2 = new Array[Double](142)
    val level_d_3 = new Array[Double](309)
    val level_d_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_d_1(i) = d(i)
    }
    for(i<-24 until 166){
      level_d_2(i-24) = d(i)
    }
    for(i<-166 until 475){
      level_d_3(i-166) = d(i)
    }
    for(i<-475 until 643){
      level_d_4(i-475) = d(i)
    }

    val Sim = math.pow(2,0)*levelSim(level_a_1,level_b_1)/(1+math.max(Entropy(level_c_1),Entropy(level_d_1))-math.min(Entropy(level_c_1),Entropy(level_d_1))) +
      math.pow(2,1)*levelSim(level_a_2,level_b_2)/(1+math.max(Entropy(level_c_2),Entropy(level_d_2))-math.min(Entropy(level_c_2),Entropy(level_d_2))) +
      math.pow(2,2)*levelSim(level_a_3,level_b_3)/(1+math.max(Entropy(level_c_3),Entropy(level_d_3))-math.min(Entropy(level_c_3),Entropy(level_d_3))) +
      math.pow(2,3)*levelSim(level_a_4,level_b_4)/(1+math.max(Entropy(level_c_4),Entropy(level_d_4))-math.min(Entropy(level_c_4),Entropy(level_d_4)))
    Sim
  }
  def hierachySim_AdjustedCosine(a:Array[Double],b:Array[Double],c:Array[Double],d:Array[Double]):Double={
    val level_a_1 = new Array[Double](24)
    val level_a_2 = new Array[Double](142)
    val level_a_3 = new Array[Double](309)
    val level_a_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_a_1(i) = a(i)
    }
    for(i<-24 until 166){
      level_a_2(i-24) = a(i)
    }
    for(i<-166 until 475){
      level_a_3(i-166) = a(i)
    }
    for(i<-475 until 643){
      level_a_4(i-475) = a(i)
    }

    val level_b_1 = new Array[Double](24)
    val level_b_2 = new Array[Double](142)
    val level_b_3 = new Array[Double](309)
    val level_b_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_b_1(i) = b(i)
    }
    for(i<-24 until 166){
      level_b_2(i-24) = b(i)
    }
    for(i<-166 until 475){
      level_b_3(i-166) = b(i)
    }
    for(i<-475 until 643){
      level_b_4(i-475) = b(i)
    }

    val level_c_1 = new Array[Double](24)
    val level_c_2 = new Array[Double](142)
    val level_c_3 = new Array[Double](309)
    val level_c_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_c_1(i) = c(i)
    }
    for(i<-24 until 166){
      level_c_2(i-24) = c(i)
    }
    for(i<-166 until 475){
      level_c_3(i-166) = c(i)
    }
    for(i<-475 until 643){
      level_c_4(i-475) = c(i)
    }

    val level_d_1 = new Array[Double](24)
    val level_d_2 = new Array[Double](142)
    val level_d_3 = new Array[Double](309)
    val level_d_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_d_1(i) = d(i)
    }
    for(i<-24 until 166){
      level_d_2(i-24) = d(i)
    }
    for(i<-166 until 475){
      level_d_3(i-166) = d(i)
    }
    for(i<-475 until 643){
      level_d_4(i-475) = d(i)
    }

    val Sim = math.pow(2,0)*AdjustedCosineSimilarity(level_a_1,level_b_1)/(1+math.max(Entropy(level_c_1),Entropy(level_d_1))-math.min(Entropy(level_c_1),Entropy(level_d_1))) +
      math.pow(2,1)*AdjustedCosineSimilarity(level_a_2,level_b_2)/(1+math.max(Entropy(level_c_2),Entropy(level_d_2))-math.min(Entropy(level_c_2),Entropy(level_d_2))) +
      math.pow(2,2)*AdjustedCosineSimilarity(level_a_3,level_b_3)/(1+math.max(Entropy(level_c_3),Entropy(level_d_3))-math.min(Entropy(level_c_3),Entropy(level_d_3))) +
      math.pow(2,3)*AdjustedCosineSimilarity(level_a_4,level_b_4)/(1+math.max(Entropy(level_c_4),Entropy(level_d_4))-math.min(Entropy(level_c_4),Entropy(level_d_4)))
    Sim
  }
  def hierachySim_Cosine(a:Array[Double],b:Array[Double],c:Array[Double],d:Array[Double]):Double={
    val level_a_1 = new Array[Double](24)
    val level_a_2 = new Array[Double](142)
    val level_a_3 = new Array[Double](309)
    val level_a_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_a_1(i) = a(i)
    }
    for(i<-24 until 166){
      level_a_2(i-24) = a(i)
    }
    for(i<-166 until 475){
      level_a_3(i-166) = a(i)
    }
    for(i<-475 until 643){
      level_a_4(i-475) = a(i)
    }

    val level_b_1 = new Array[Double](24)
    val level_b_2 = new Array[Double](142)
    val level_b_3 = new Array[Double](309)
    val level_b_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_b_1(i) = b(i)
    }
    for(i<-24 until 166){
      level_b_2(i-24) = b(i)
    }
    for(i<-166 until 475){
      level_b_3(i-166) = b(i)
    }
    for(i<-475 until 643){
      level_b_4(i-475) = b(i)
    }

    val level_c_1 = new Array[Double](24)
    val level_c_2 = new Array[Double](142)
    val level_c_3 = new Array[Double](309)
    val level_c_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_c_1(i) = c(i)
    }
    for(i<-24 until 166){
      level_c_2(i-24) = c(i)
    }
    for(i<-166 until 475){
      level_c_3(i-166) = c(i)
    }
    for(i<-475 until 643){
      level_c_4(i-475) = c(i)
    }

    val level_d_1 = new Array[Double](24)
    val level_d_2 = new Array[Double](142)
    val level_d_3 = new Array[Double](309)
    val level_d_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_d_1(i) = d(i)
    }
    for(i<-24 until 166){
      level_d_2(i-24) = d(i)
    }
    for(i<-166 until 475){
      level_d_3(i-166) = d(i)
    }
    for(i<-475 until 643){
      level_d_4(i-475) = d(i)
    }

    val Sim = math.pow(2,0)*CosineCoefficientSimilarity(level_a_1,level_b_1)/(1+math.max(Entropy(level_c_1),Entropy(level_d_1))-math.min(Entropy(level_c_1),Entropy(level_d_1))) +
      math.pow(2,1)*CosineCoefficientSimilarity(level_a_2,level_b_2)/(1+math.max(Entropy(level_c_2),Entropy(level_d_2))-math.min(Entropy(level_c_2),Entropy(level_d_2))) +
      math.pow(2,2)*CosineCoefficientSimilarity(level_a_3,level_b_3)/(1+math.max(Entropy(level_c_3),Entropy(level_d_3))-math.min(Entropy(level_c_3),Entropy(level_d_3))) +
      math.pow(2,3)*CosineCoefficientSimilarity(level_a_4,level_b_4)/(1+math.max(Entropy(level_c_4),Entropy(level_d_4))-math.min(Entropy(level_c_4),Entropy(level_d_4)))
    Sim
  }
  def hierachySim_Pearson(a:Array[Double],b:Array[Double],c:Array[Double],d:Array[Double]):Double={
    val level_a_1 = new Array[Double](24)
    val level_a_2 = new Array[Double](142)
    val level_a_3 = new Array[Double](309)
    val level_a_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_a_1(i) = a(i)
    }
    for(i<-24 until 166){
      level_a_2(i-24) = a(i)
    }
    for(i<-166 until 475){
      level_a_3(i-166) = a(i)
    }
    for(i<-475 until 643){
      level_a_4(i-475) = a(i)
    }

    val level_b_1 = new Array[Double](24)
    val level_b_2 = new Array[Double](142)
    val level_b_3 = new Array[Double](309)
    val level_b_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_b_1(i) = b(i)
    }
    for(i<-24 until 166){
      level_b_2(i-24) = b(i)
    }
    for(i<-166 until 475){
      level_b_3(i-166) = b(i)
    }
    for(i<-475 until 643){
      level_b_4(i-475) = b(i)
    }

    val level_c_1 = new Array[Double](24)
    val level_c_2 = new Array[Double](142)
    val level_c_3 = new Array[Double](309)
    val level_c_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_c_1(i) = c(i)
    }
    for(i<-24 until 166){
      level_c_2(i-24) = c(i)
    }
    for(i<-166 until 475){
      level_c_3(i-166) = c(i)
    }
    for(i<-475 until 643){
      level_c_4(i-475) = c(i)
    }

    val level_d_1 = new Array[Double](24)
    val level_d_2 = new Array[Double](142)
    val level_d_3 = new Array[Double](309)
    val level_d_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_d_1(i) = d(i)
    }
    for(i<-24 until 166){
      level_d_2(i-24) = d(i)
    }
    for(i<-166 until 475){
      level_d_3(i-166) = d(i)
    }
    for(i<-475 until 643){
      level_d_4(i-475) = d(i)
    }

    val Sim = math.pow(2,0)*PearsonCorrelationCoefficient(level_a_1,level_b_1)/(1+math.max(Entropy(level_c_1),Entropy(level_d_1))-math.min(Entropy(level_c_1),Entropy(level_d_1))) +
      math.pow(2,1)*PearsonCorrelationCoefficient(level_a_2,level_b_2)/(1+math.max(Entropy(level_c_2),Entropy(level_d_2))-math.min(Entropy(level_c_2),Entropy(level_d_2))) +
      math.pow(2,2)*PearsonCorrelationCoefficient(level_a_3,level_b_3)/(1+math.max(Entropy(level_c_3),Entropy(level_d_3))-math.min(Entropy(level_c_3),Entropy(level_d_3))) +
      math.pow(2,3)*PearsonCorrelationCoefficient(level_a_4,level_b_4)/(1+math.max(Entropy(level_c_4),Entropy(level_d_4))-math.min(Entropy(level_c_4),Entropy(level_d_4)))
    Sim
  }
  def hierachySim_Tanimoto(a:Array[Double],b:Array[Double],c:Array[Double],d:Array[Double]):Double={
    val level_a_1 = new Array[Double](24)
    val level_a_2 = new Array[Double](142)
    val level_a_3 = new Array[Double](309)
    val level_a_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_a_1(i) = a(i)
    }
    for(i<-24 until 166){
      level_a_2(i-24) = a(i)
    }
    for(i<-166 until 475){
      level_a_3(i-166) = a(i)
    }
    for(i<-475 until 643){
      level_a_4(i-475) = a(i)
    }

    val level_b_1 = new Array[Double](24)
    val level_b_2 = new Array[Double](142)
    val level_b_3 = new Array[Double](309)
    val level_b_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_b_1(i) = b(i)
    }
    for(i<-24 until 166){
      level_b_2(i-24) = b(i)
    }
    for(i<-166 until 475){
      level_b_3(i-166) = b(i)
    }
    for(i<-475 until 643){
      level_b_4(i-475) = b(i)
    }

    val level_c_1 = new Array[Double](24)
    val level_c_2 = new Array[Double](142)
    val level_c_3 = new Array[Double](309)
    val level_c_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_c_1(i) = c(i)
    }
    for(i<-24 until 166){
      level_c_2(i-24) = c(i)
    }
    for(i<-166 until 475){
      level_c_3(i-166) = c(i)
    }
    for(i<-475 until 643){
      level_c_4(i-475) = c(i)
    }

    val level_d_1 = new Array[Double](24)
    val level_d_2 = new Array[Double](142)
    val level_d_3 = new Array[Double](309)
    val level_d_4 = new Array[Double](168)

    for(i<-0 until 24){
      level_d_1(i) = d(i)
    }
    for(i<-24 until 166){
      level_d_2(i-24) = d(i)
    }
    for(i<-166 until 475){
      level_d_3(i-166) = d(i)
    }
    for(i<-475 until 643){
      level_d_4(i-475) = d(i)
    }

    val Sim = math.pow(2,0)*TanimotoCoefficent(level_a_1,level_b_1)/(1+math.max(Entropy(level_c_1),Entropy(level_d_1))-math.min(Entropy(level_c_1),Entropy(level_d_1))) +
      math.pow(2,1)*TanimotoCoefficent(level_a_2,level_b_2)/(1+math.max(Entropy(level_c_2),Entropy(level_d_2))-math.min(Entropy(level_c_2),Entropy(level_d_2))) +
      math.pow(2,2)*TanimotoCoefficent(level_a_3,level_b_3)/(1+math.max(Entropy(level_c_3),Entropy(level_d_3))-math.min(Entropy(level_c_3),Entropy(level_d_3))) +
      math.pow(2,3)*TanimotoCoefficent(level_a_4,level_b_4)/(1+math.max(Entropy(level_c_4),Entropy(level_d_4))-math.min(Entropy(level_c_4),Entropy(level_d_4)))
    Sim
  }
  /**
   * AdjustedCosineSimilarity
   * @param a:Array[Double]
   * @param b:Array[Double
   * @return Double
   */
  def AdjustedCosineSimilarity(a:Array[Double],b:Array[Double]):Double={
    if(a.length!=b.length){
      0.0
    }else{
      val l = a.length
      var adMulDis = 0.0
      var adPreDis = 0.0
      var adCurDis = 0.0
      val aMean = a.sum/l
      val bMean = b.sum/l
      var adPre = 0.0
      var adCur = 0.0
      for(i<-0 until l){
        adPre = a(i)-aMean
        adCur = b(i)-bMean
        adMulDis = adMulDis + adPre*adCur
        adPreDis = adPreDis + adPre*adPre
        adCurDis = adCurDis + adCur*adCur
      }
      if(adPreDis == 0.0 || adCurDis == 0.0){
        0.0
      }else {
        val distance = adMulDis / (sqrt(adPreDis * adCurDis))
        distance
      }
    }
  }

  /**
   * AdjustedCosineSimilarity regarless of the zero appearing in the same position of a and b
   * @param a:Array[Double]
   * @param b:Array[Double
   * @return Double
   */
  def AdjustedCosineSimilarityWithoutZeros(a:Array[Double],b:Array[Double]):Double={
    if(a.length!=b.length){
      0.0
    }else {
      val l = a.length
      var ll = 0
      for(i<-0 until l){
        if(a(i)!=0.0 && b(i)!=0.0){
          ll = ll+1
        }
      }
      val X = new Array[Double](ll)
      val Y = new Array[Double](ll)
      var m = 0
      for(i<-0 until l if m<ll){
        if(a(i)!=0.0 && b(i)!=0.0){
          X(m) = a(i)
          Y(m) = b(i)
          m = m+1
        }
      }
      AdjustedCosineSimilarity(X,Y)
    }
  }

  /**
   * CosineSimilariy
   * @param a;Array[Double]
   * @param b:Array[Double]
   * @return Double
   */
  def CosineCoefficientSimilarity(a:Array[Double],b:Array[Double]):Double = {
    if(a.length!=b.length){
      0.0
    }else{
      val l = a.length
      var MulDis = 0.0
      var PreDis = 0.0
      var CurDis = 0.0
      for(i<-0 until l){
        MulDis = MulDis + a(i)*b(i)
        PreDis = PreDis + a(i)*a(i)
        CurDis = CurDis + b(i)*b(i)
      }
      if(PreDis == 0.0 || CurDis == 0.0){
        0.0
      }else {
        val distance = MulDis / (sqrt(PreDis * CurDis))
        distance
      }
    }
  }

  /**
   * PearsonCorrelationSimilarity range[-1,1]
   * reprensent the linear correlation between two Arrays
   * @param a:Array[Double]
   * @param b:Array[Double]
   * @return Double
   */
  def PearsonCorrelationCoefficient(a:Array[Double],b:Array[Double]):Double={
    if(a.length!=b.length){
      0.0
    }else{
      val l = a.length
      val aSum = a.sum
      val bSum = b.sum
      var mul = 0.0
      var aSqu = 0.0
      var bSqu = 0.0
      for(i<-0 until l){
        mul = mul + a(i)*b(i)
        aSqu = aSqu + a(i)*a(i)
        bSqu = bSqu + b(i)*b(i)
      }
      val mulDis = l*mul-aSum*bSum
      val aDis = l*aSqu-aSum*aSum
      val bDis = l*bSqu-bSum*bSum
      if(aDis == 0.0 || bDis == 0.0){
        0.0
      }else{
        val coefficient = mulDis / sqrt(aDis*bDis)
        coefficient
      }
    }
  }

  /**
   * JaccardCoefficient
   * @param a:Array[Double]
   * @param b:Array[Double]
   * @return Double
   */
  def JaccardCoefficent(a:Array[Double],b:Array[Double]):Double = {
    if(a.length!=b.length){
      0.0
    }else{
      val l = a.length
      var interSet = 0.0
      var unionSet = 0.0
      for(i<-0 until l){
        interSet = interSet + min(a(i),b(i))
        unionSet = unionSet + max(a(i),b(i))
      }
      if(unionSet == 0.0){
        -1.0
      }else{
        interSet/unionSet
      }
    }
  }

  /**
   * TanimotoCoeffient
   * @param a:Array[Double]
   * @param b;Array[Double]
   * @return Double
   */
  def TanimotoCoefficent(a:Array[Double],b:Array[Double]):Double ={
    if(a.length!=b.length){
      0.0
    }else{
      val l = a.length
      var mul = 0.0
      var aDis = 0.0
      var bDis = 0.0
      for(i<-0 until l){
        mul = mul + a(i)*b(i)
        aDis = aDis + a(i)*a(i)
        bDis = bDis + b(i)*b(i)
      }
      val under = aDis+bDis-mul
      if(under == 0.0){
        0.0
      }else{
        val Dis = mul/under
        Dis
      }
    }
  }

  /**
   * EuclideanDistanceSimilarity
   * @param a:Array[Double]
   * @param b;Array[Double]
   * @return Double
   */
  def EuclideanDistanceSimilarity(a:Array[Double],b:Array[Double]):Double={
    ZeroOneTransform(EuclideanDistance(a,b))
  }

  /**
   * ManhattanDistanceSimilarity
   * @param a:Array[Double]
   * @param b:Array[Double]
   * @return Double
   */
  def ManhattanDistanceSimilarity(a:Array[Double],b:Array[Double]):Double={
    ZeroOneTransform(ManhattanDistance(a,b))
  }
  //Special Similarity
  /**
   * All+C23+L4 AdjustedCosineSimilarity
   * @param a;Array[Double] length = 643
   * @param b:Array[Double] length = 643
   * @return Array[Double] length = 28
   */
  def AdjustedCosineSimilarity4Interest(a:Array[Double],b:Array[Double]):Array[Double]={
    if(a.length!=b.length){
      val result = new Array[Double](28)
      result
    }else {
      val l = a.length
      var ll = 0
      val result = new Array[Double](28)
      result(13) = 0.0
      val X = new Array[Double](InterestVector.Ll)
      val Y = new Array[Double](InterestVector.Ll)
      val Xl1 = new Array[Double](InterestVector.Ll1)
      val Xl2 = new Array[Double](InterestVector.Ll2)
      val Xl3 = new Array[Double](InterestVector.Ll3)
      val Xl4 = new Array[Double](InterestVector.Ll4)
      val Xc1 = new Array[Double](InterestVector.Lc1)
      val Xc2 = new Array[Double](InterestVector.Lc2)
      val Xc3 = new Array[Double](InterestVector.Lc3)
      val Xc4 = new Array[Double](InterestVector.Lc4)
      val Xc5 = new Array[Double](InterestVector.Lc5)
      val Xc6 = new Array[Double](InterestVector.Lc6)
      val Xc7 = new Array[Double](InterestVector.Lc7)
      val Xc8 = new Array[Double](InterestVector.Lc8)
      val Xc9 = new Array[Double](InterestVector.Lc9)
      val Xc10 = new Array[Double](InterestVector.Lc10)
      val Xc11 = new Array[Double](InterestVector.Lc11)
      val Xc12 = new Array[Double](InterestVector.Lc12)
      val Xc13 = new Array[Double](InterestVector.Lc13)
      val Xc14 = new Array[Double](InterestVector.Lc14)
      val Xc15 = new Array[Double](InterestVector.Lc15)
      val Xc16 = new Array[Double](InterestVector.Lc16)
      val Xc17 = new Array[Double](InterestVector.Lc17)
      val Xc18 = new Array[Double](InterestVector.Lc18)
      val Xc19 = new Array[Double](InterestVector.Lc19)
      val Xc20 = new Array[Double](InterestVector.Lc20)
      val Xc21 = new Array[Double](InterestVector.Lc21)
      val Xc22 = new Array[Double](InterestVector.Lc22)
      val Xc23 = new Array[Double](InterestVector.Lc23)
      val Yl1 = new Array[Double](InterestVector.Ll1)
      val Yl2 = new Array[Double](InterestVector.Ll2)
      val Yl3 = new Array[Double](InterestVector.Ll3)
      val Yl4 = new Array[Double](InterestVector.Ll4)
      val Yc1 = new Array[Double](InterestVector.Lc1)
      val Yc2 = new Array[Double](InterestVector.Lc2)
      val Yc3 = new Array[Double](InterestVector.Lc3)
      val Yc4 = new Array[Double](InterestVector.Lc4)
      val Yc5 = new Array[Double](InterestVector.Lc5)
      val Yc6 = new Array[Double](InterestVector.Lc6)
      val Yc7 = new Array[Double](InterestVector.Lc7)
      val Yc8 = new Array[Double](InterestVector.Lc8)
      val Yc9 = new Array[Double](InterestVector.Lc9)
      val Yc10 = new Array[Double](InterestVector.Lc10)
      val Yc11 = new Array[Double](InterestVector.Lc11)
      val Yc12 = new Array[Double](InterestVector.Lc12)
      val Yc13 = new Array[Double](InterestVector.Lc13)
      val Yc14 = new Array[Double](InterestVector.Lc14)
      val Yc15 = new Array[Double](InterestVector.Lc15)
      val Yc16 = new Array[Double](InterestVector.Lc16)
      val Yc17 = new Array[Double](InterestVector.Lc17)
      val Yc18 = new Array[Double](InterestVector.Lc18)
      val Yc19 = new Array[Double](InterestVector.Lc19)
      val Yc20 = new Array[Double](InterestVector.Lc20)
      val Yc21 = new Array[Double](InterestVector.Lc21)
      val Yc22 = new Array[Double](InterestVector.Lc22)
      val Yc23 = new Array[Double](InterestVector.Lc23)
      for(i<-0 until InterestVector.Ll1){
        Xl1(i) = a(InterestVector.L1(i))
        Yl1(i) = b(InterestVector.L1(i))
        X(i) = a(InterestVector.L1(i))
        Y(i) = b(InterestVector.L1(i))
      }
      for(i<-0 until InterestVector.Ll2){
        Xl2(i) = a(InterestVector.L2(i))
        Yl2(i) = b(InterestVector.L2(i))
        X(i+InterestVector.Ll1) = a(InterestVector.L2(i))
        Y(i+InterestVector.Ll1) = b(InterestVector.L2(i))
      }
      for(i<-0 until InterestVector.Ll3){
        Xl3(i) = a(InterestVector.L3(i))
        Yl3(i) = b(InterestVector.L3(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2) = a(InterestVector.L3(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2) = b(InterestVector.L3(i))
      }
      for(i<-0 until InterestVector.Ll4){
        Xl4(i) = a(InterestVector.L4(i))
        Yl4(i) = b(InterestVector.L4(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = a(InterestVector.L4(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = b(InterestVector.L4(i))
      }
      for(i<-0 until InterestVector.Lc1){
        Xc1(i) = a(InterestVector.C1(i))
        Yc1(i) = b(InterestVector.C1(i))
      }
      for(i<-0 until InterestVector.Lc2){
        Xc2(i) = a(InterestVector.C2(i))
        Yc2(i) = b(InterestVector.C2(i))
      }
      for(i<-0 until InterestVector.Lc3){
        Xc3(i) = a(InterestVector.C3(i))
        Yc3(i) = b(InterestVector.C3(i))
      }
      for(i<-0 until InterestVector.Lc4){
        Xc4(i) = a(InterestVector.C4(i))
        Yc4(i) = b(InterestVector.C4(i))
      }
      for(i<-0 until InterestVector.Lc5){
        Xc5(i) = a(InterestVector.C5(i))
        Yc5(i) = b(InterestVector.C5(i))
      }
      for(i<-0 until InterestVector.Lc6){
        Xc6(i) = a(InterestVector.C6(i))
        Yc6(i) = b(InterestVector.C6(i))
      }
      for(i<-0 until InterestVector.Lc7){
        Xc7(i) = a(InterestVector.C7(i))
        Yc7(i) = b(InterestVector.C7(i))
      }
      for(i<-0 until InterestVector.Lc8){
        Xc8(i) = a(InterestVector.C8(i))
        Yc8(i) = b(InterestVector.C8(i))
      }
      for(i<-0 until InterestVector.Lc9){
        Xc9(i) = a(InterestVector.C9(i))
        Yc9(i) = b(InterestVector.C9(i))
      }
      for(i<-0 until InterestVector.Lc10){
        Xc10(i) = a(InterestVector.C10(i))
        Yc10(i) = b(InterestVector.C10(i))
      }
      for(i<-0 until InterestVector.Lc11){
        Xc11(i) = a(InterestVector.C11(i))
        Yc11(i) = b(InterestVector.C11(i))
      }
      for(i<-0 until InterestVector.Lc12){
        Xc12(i) = a(InterestVector.C12(i))
        Yc12(i) = b(InterestVector.C12(i))
      }
      for(i<-0 until InterestVector.Lc13){
        Xc13(i) = a(InterestVector.C13(i))
        Yc13(i) = b(InterestVector.C13(i))
      }
      for(i<-0 until InterestVector.Lc14){
        Xc14(i) = a(InterestVector.C14(i))
        Yc14(i) = b(InterestVector.C14(i))
      }
      for(i<-0 until InterestVector.Lc15){
        Xc15(i) = a(InterestVector.C15(i))
        Yc15(i) = b(InterestVector.C15(i))
      }
      for(i<-0 until InterestVector.Lc16){
        Xc16(i) = a(InterestVector.C16(i))
        Yc16(i) = b(InterestVector.C16(i))
      }
      for(i<-0 until InterestVector.Lc17){
        Xc17(i) = a(InterestVector.C17(i))
        Yc17(i) = b(InterestVector.C17(i))
      }
      for(i<-0 until InterestVector.Lc18){
        Xc18(i) = a(InterestVector.C18(i))
        Yc18(i) = b(InterestVector.C18(i))
      }
      for(i<-0 until InterestVector.Lc19){
        Xc19(i) = a(InterestVector.C19(i))
        Yc19(i) = b(InterestVector.C19(i))
      }
      for(i<-0 until InterestVector.Lc20){
        Xc20(i) = a(InterestVector.C20(i))
        Yc20(i) = b(InterestVector.C20(i))
      }
      for(i<-0 until InterestVector.Lc21){
        Xc21(i) = a(InterestVector.C21(i))
        Yc21(i) = b(InterestVector.C21(i))
      }
      for(i<-0 until InterestVector.Lc22){
        Xc22(i) = a(InterestVector.C22(i))
        Yc22(i) = b(InterestVector.C22(i))
      }
      for(i<-0 until InterestVector.Lc23){
        Xc23(i) = a(InterestVector.C23(i))
        Yc23(i) = b(InterestVector.C23(i))
      }
      result(0) = AdjustedCosineSimilarity(X,Y)
      result(1) = AdjustedCosineSimilarity(Xc1,Yc1)
      result(2) = AdjustedCosineSimilarity(Xc2,Yc2)
      result(3) = AdjustedCosineSimilarity(Xc3,Yc3)
      result(4) = AdjustedCosineSimilarity(Xc4,Yc4)
      result(5) = AdjustedCosineSimilarity(Xc5,Yc5)
      result(6) = AdjustedCosineSimilarity(Xc6,Yc6)
      result(7) = AdjustedCosineSimilarity(Xc7,Yc7)
      result(8) = AdjustedCosineSimilarity(Xc8,Yc8)
      result(9) = AdjustedCosineSimilarity(Xc9,Yc9)
      result(10) = AdjustedCosineSimilarity(Xc10,Yc10)
      result(11) = AdjustedCosineSimilarity(Xc11,Yc11)
      result(12) = AdjustedCosineSimilarity(Xc12,Yc12)
      result(14) = AdjustedCosineSimilarity(Xc14,Yc14)
      result(15) = AdjustedCosineSimilarity(Xc15,Yc15)
      result(16) = AdjustedCosineSimilarity(Xc16,Yc16)
      result(17) = AdjustedCosineSimilarity(Xc17,Yc17)
      result(18) = AdjustedCosineSimilarity(Xc18,Yc18)
      result(19) = AdjustedCosineSimilarity(Xc19,Yc19)
      result(20) = AdjustedCosineSimilarity(Xc20,Yc20)
      result(21) = AdjustedCosineSimilarity(Xc21,Yc21)
      result(22) = AdjustedCosineSimilarity(Xc22,Yc22)
      result(23) = AdjustedCosineSimilarity(Xc23,Yc23)
      result(24) = AdjustedCosineSimilarity(Xl1,Yl1)
      result(25) = AdjustedCosineSimilarity(Xl2,Yl2)
      result(26) = AdjustedCosineSimilarity(Xl3,Yl3)
      result(27) = AdjustedCosineSimilarity(Xl4,Yl4)
      result
    }
  }

  /**
   * All+C23+L4 AdjustedCosineSimilarity regarless of the zero appearing in the same position of a and b
   * @param a;Array[Double] length = 643
   * @param b:Array[Double] length = 643
   * @return Array[Double] length = 28
   */
  def AdjustedCosineSimilarity4InterestKZ(a:Array[Double],b:Array[Double]):Array[Double]={
    if(a.length!=b.length){
      val result = new Array[Double](28)
      result
    }else {
      val l = a.length
      var ll = 0
      val result = new Array[Double](28)
      result(13) = 0.0
      val X = new Array[Double](InterestVector.Ll)
      val Y = new Array[Double](InterestVector.Ll)
      val Xl1 = new Array[Double](InterestVector.Ll1)
      val Xl2 = new Array[Double](InterestVector.Ll2)
      val Xl3 = new Array[Double](InterestVector.Ll3)
      val Xl4 = new Array[Double](InterestVector.Ll4)
      val Xc1 = new Array[Double](InterestVector.Lc1)
      val Xc2 = new Array[Double](InterestVector.Lc2)
      val Xc3 = new Array[Double](InterestVector.Lc3)
      val Xc4 = new Array[Double](InterestVector.Lc4)
      val Xc5 = new Array[Double](InterestVector.Lc5)
      val Xc6 = new Array[Double](InterestVector.Lc6)
      val Xc7 = new Array[Double](InterestVector.Lc7)
      val Xc8 = new Array[Double](InterestVector.Lc8)
      val Xc9 = new Array[Double](InterestVector.Lc9)
      val Xc10 = new Array[Double](InterestVector.Lc10)
      val Xc11 = new Array[Double](InterestVector.Lc11)
      val Xc12 = new Array[Double](InterestVector.Lc12)
      val Xc13 = new Array[Double](InterestVector.Lc13)
      val Xc14 = new Array[Double](InterestVector.Lc14)
      val Xc15 = new Array[Double](InterestVector.Lc15)
      val Xc16 = new Array[Double](InterestVector.Lc16)
      val Xc17 = new Array[Double](InterestVector.Lc17)
      val Xc18 = new Array[Double](InterestVector.Lc18)
      val Xc19 = new Array[Double](InterestVector.Lc19)
      val Xc20 = new Array[Double](InterestVector.Lc20)
      val Xc21 = new Array[Double](InterestVector.Lc21)
      val Xc22 = new Array[Double](InterestVector.Lc22)
      val Xc23 = new Array[Double](InterestVector.Lc23)
      val Yl1 = new Array[Double](InterestVector.Ll1)
      val Yl2 = new Array[Double](InterestVector.Ll2)
      val Yl3 = new Array[Double](InterestVector.Ll3)
      val Yl4 = new Array[Double](InterestVector.Ll4)
      val Yc1 = new Array[Double](InterestVector.Lc1)
      val Yc2 = new Array[Double](InterestVector.Lc2)
      val Yc3 = new Array[Double](InterestVector.Lc3)
      val Yc4 = new Array[Double](InterestVector.Lc4)
      val Yc5 = new Array[Double](InterestVector.Lc5)
      val Yc6 = new Array[Double](InterestVector.Lc6)
      val Yc7 = new Array[Double](InterestVector.Lc7)
      val Yc8 = new Array[Double](InterestVector.Lc8)
      val Yc9 = new Array[Double](InterestVector.Lc9)
      val Yc10 = new Array[Double](InterestVector.Lc10)
      val Yc11 = new Array[Double](InterestVector.Lc11)
      val Yc12 = new Array[Double](InterestVector.Lc12)
      val Yc13 = new Array[Double](InterestVector.Lc13)
      val Yc14 = new Array[Double](InterestVector.Lc14)
      val Yc15 = new Array[Double](InterestVector.Lc15)
      val Yc16 = new Array[Double](InterestVector.Lc16)
      val Yc17 = new Array[Double](InterestVector.Lc17)
      val Yc18 = new Array[Double](InterestVector.Lc18)
      val Yc19 = new Array[Double](InterestVector.Lc19)
      val Yc20 = new Array[Double](InterestVector.Lc20)
      val Yc21 = new Array[Double](InterestVector.Lc21)
      val Yc22 = new Array[Double](InterestVector.Lc22)
      val Yc23 = new Array[Double](InterestVector.Lc23)
      for(i<-0 until InterestVector.Ll1){
        Xl1(i) = a(InterestVector.L1(i))
        Yl1(i) = b(InterestVector.L1(i))
        X(i) = a(InterestVector.L1(i))
        Y(i) = b(InterestVector.L1(i))
      }
      for(i<-0 until InterestVector.Ll2){
        Xl2(i) = a(InterestVector.L2(i))
        Yl2(i) = b(InterestVector.L2(i))
        X(i+InterestVector.Ll1) = a(InterestVector.L2(i))
        Y(i+InterestVector.Ll1) = b(InterestVector.L2(i))
      }
      for(i<-0 until InterestVector.Ll3){
        Xl3(i) = a(InterestVector.L3(i))
        Yl3(i) = b(InterestVector.L3(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2) = a(InterestVector.L3(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2) = b(InterestVector.L3(i))
      }
      for(i<-0 until InterestVector.Ll4){
        Xl4(i) = a(InterestVector.L4(i))
        Yl4(i) = b(InterestVector.L4(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = a(InterestVector.L4(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = b(InterestVector.L4(i))
      }
      for(i<-0 until InterestVector.Lc1){
        Xc1(i) = a(InterestVector.C1(i))
        Yc1(i) = b(InterestVector.C1(i))
      }
      for(i<-0 until InterestVector.Lc2){
        Xc2(i) = a(InterestVector.C2(i))
        Yc2(i) = b(InterestVector.C2(i))
      }
      for(i<-0 until InterestVector.Lc3){
        Xc3(i) = a(InterestVector.C3(i))
        Yc3(i) = b(InterestVector.C3(i))
      }
      for(i<-0 until InterestVector.Lc4){
        Xc4(i) = a(InterestVector.C4(i))
        Yc4(i) = b(InterestVector.C4(i))
      }
      for(i<-0 until InterestVector.Lc5){
        Xc5(i) = a(InterestVector.C5(i))
        Yc5(i) = b(InterestVector.C5(i))
      }
      for(i<-0 until InterestVector.Lc6){
        Xc6(i) = a(InterestVector.C6(i))
        Yc6(i) = b(InterestVector.C6(i))
      }
      for(i<-0 until InterestVector.Lc7){
        Xc7(i) = a(InterestVector.C7(i))
        Yc7(i) = b(InterestVector.C7(i))
      }
      for(i<-0 until InterestVector.Lc8){
        Xc8(i) = a(InterestVector.C8(i))
        Yc8(i) = b(InterestVector.C8(i))
      }
      for(i<-0 until InterestVector.Lc9){
        Xc9(i) = a(InterestVector.C9(i))
        Yc9(i) = b(InterestVector.C9(i))
      }
      for(i<-0 until InterestVector.Lc10){
        Xc10(i) = a(InterestVector.C10(i))
        Yc10(i) = b(InterestVector.C10(i))
      }
      for(i<-0 until InterestVector.Lc11){
        Xc11(i) = a(InterestVector.C11(i))
        Yc11(i) = b(InterestVector.C11(i))
      }
      for(i<-0 until InterestVector.Lc12){
        Xc12(i) = a(InterestVector.C12(i))
        Yc12(i) = b(InterestVector.C12(i))
      }
      for(i<-0 until InterestVector.Lc13){
        Xc13(i) = a(InterestVector.C13(i))
        Yc13(i) = b(InterestVector.C13(i))
      }
      for(i<-0 until InterestVector.Lc14){
        Xc14(i) = a(InterestVector.C14(i))
        Yc14(i) = b(InterestVector.C14(i))
      }
      for(i<-0 until InterestVector.Lc15){
        Xc15(i) = a(InterestVector.C15(i))
        Yc15(i) = b(InterestVector.C15(i))
      }
      for(i<-0 until InterestVector.Lc16){
        Xc16(i) = a(InterestVector.C16(i))
        Yc16(i) = b(InterestVector.C16(i))
      }
      for(i<-0 until InterestVector.Lc17){
        Xc17(i) = a(InterestVector.C17(i))
        Yc17(i) = b(InterestVector.C17(i))
      }
      for(i<-0 until InterestVector.Lc18){
        Xc18(i) = a(InterestVector.C18(i))
        Yc18(i) = b(InterestVector.C18(i))
      }
      for(i<-0 until InterestVector.Lc19){
        Xc19(i) = a(InterestVector.C19(i))
        Yc19(i) = b(InterestVector.C19(i))
      }
      for(i<-0 until InterestVector.Lc20){
        Xc20(i) = a(InterestVector.C20(i))
        Yc20(i) = b(InterestVector.C20(i))
      }
      for(i<-0 until InterestVector.Lc21){
        Xc21(i) = a(InterestVector.C21(i))
        Yc21(i) = b(InterestVector.C21(i))
      }
      for(i<-0 until InterestVector.Lc22){
        Xc22(i) = a(InterestVector.C22(i))
        Yc22(i) = b(InterestVector.C22(i))
      }
      for(i<-0 until InterestVector.Lc23){
        Xc23(i) = a(InterestVector.C23(i))
        Yc23(i) = b(InterestVector.C23(i))
      }
      result(0) = AdjustedCosineSimilarityWithoutZeros(X,Y)
      result(1) = AdjustedCosineSimilarityWithoutZeros(Xc1,Yc1)
      result(2) = AdjustedCosineSimilarityWithoutZeros(Xc2,Yc2)
      result(3) = AdjustedCosineSimilarityWithoutZeros(Xc3,Yc3)
      result(4) = AdjustedCosineSimilarityWithoutZeros(Xc4,Yc4)
      result(5) = AdjustedCosineSimilarityWithoutZeros(Xc5,Yc5)
      result(6) = AdjustedCosineSimilarityWithoutZeros(Xc6,Yc6)
      result(7) = AdjustedCosineSimilarityWithoutZeros(Xc7,Yc7)
      result(8) = AdjustedCosineSimilarityWithoutZeros(Xc8,Yc8)
      result(9) = AdjustedCosineSimilarityWithoutZeros(Xc9,Yc9)
      result(10) = AdjustedCosineSimilarityWithoutZeros(Xc10,Yc10)
      result(11) = AdjustedCosineSimilarityWithoutZeros(Xc11,Yc11)
      result(12) = AdjustedCosineSimilarityWithoutZeros(Xc12,Yc12)
      result(14) = AdjustedCosineSimilarityWithoutZeros(Xc14,Yc14)
      result(15) = AdjustedCosineSimilarityWithoutZeros(Xc15,Yc15)
      result(16) = AdjustedCosineSimilarityWithoutZeros(Xc16,Yc16)
      result(17) = AdjustedCosineSimilarityWithoutZeros(Xc17,Yc17)
      result(18) = AdjustedCosineSimilarityWithoutZeros(Xc18,Yc18)
      result(19) = AdjustedCosineSimilarityWithoutZeros(Xc19,Yc19)
      result(20) = AdjustedCosineSimilarityWithoutZeros(Xc20,Yc20)
      result(21) = AdjustedCosineSimilarityWithoutZeros(Xc21,Yc21)
      result(22) = AdjustedCosineSimilarityWithoutZeros(Xc22,Yc22)
      result(23) = AdjustedCosineSimilarityWithoutZeros(Xc23,Yc23)
      result(24) = AdjustedCosineSimilarityWithoutZeros(Xl1,Yl1)
      result(25) = AdjustedCosineSimilarityWithoutZeros(Xl2,Yl2)
      result(26) = AdjustedCosineSimilarityWithoutZeros(Xl3,Yl3)
      result(27) = AdjustedCosineSimilarityWithoutZeros(Xl4,Yl4)
      result
    }
  }

  /**
   * All+C23+L4 CosineSimilarity
   * @param a;Array[Double] length = 643
   * @param b:Array[Double] length = 643
   * @return Array[Double] length = 28
   */
  def CosineCoefficientSimilarity4Interest(a:Array[Double],b:Array[Double]):Array[Double]={
    if(a.length!=b.length){
      val result = new Array[Double](28)
      result
    }else {
      val l = a.length
      var ll = 0
      val result = new Array[Double](28)
      result(13) = 0.0
      val X = new Array[Double](InterestVector.Ll)
      val Y = new Array[Double](InterestVector.Ll)
      val Xl1 = new Array[Double](InterestVector.Ll1)
      val Xl2 = new Array[Double](InterestVector.Ll2)
      val Xl3 = new Array[Double](InterestVector.Ll3)
      val Xl4 = new Array[Double](InterestVector.Ll4)
      val Xc1 = new Array[Double](InterestVector.Lc1)
      val Xc2 = new Array[Double](InterestVector.Lc2)
      val Xc3 = new Array[Double](InterestVector.Lc3)
      val Xc4 = new Array[Double](InterestVector.Lc4)
      val Xc5 = new Array[Double](InterestVector.Lc5)
      val Xc6 = new Array[Double](InterestVector.Lc6)
      val Xc7 = new Array[Double](InterestVector.Lc7)
      val Xc8 = new Array[Double](InterestVector.Lc8)
      val Xc9 = new Array[Double](InterestVector.Lc9)
      val Xc10 = new Array[Double](InterestVector.Lc10)
      val Xc11 = new Array[Double](InterestVector.Lc11)
      val Xc12 = new Array[Double](InterestVector.Lc12)
      val Xc13 = new Array[Double](InterestVector.Lc13)
      val Xc14 = new Array[Double](InterestVector.Lc14)
      val Xc15 = new Array[Double](InterestVector.Lc15)
      val Xc16 = new Array[Double](InterestVector.Lc16)
      val Xc17 = new Array[Double](InterestVector.Lc17)
      val Xc18 = new Array[Double](InterestVector.Lc18)
      val Xc19 = new Array[Double](InterestVector.Lc19)
      val Xc20 = new Array[Double](InterestVector.Lc20)
      val Xc21 = new Array[Double](InterestVector.Lc21)
      val Xc22 = new Array[Double](InterestVector.Lc22)
      val Xc23 = new Array[Double](InterestVector.Lc23)
      val Yl1 = new Array[Double](InterestVector.Ll1)
      val Yl2 = new Array[Double](InterestVector.Ll2)
      val Yl3 = new Array[Double](InterestVector.Ll3)
      val Yl4 = new Array[Double](InterestVector.Ll4)
      val Yc1 = new Array[Double](InterestVector.Lc1)
      val Yc2 = new Array[Double](InterestVector.Lc2)
      val Yc3 = new Array[Double](InterestVector.Lc3)
      val Yc4 = new Array[Double](InterestVector.Lc4)
      val Yc5 = new Array[Double](InterestVector.Lc5)
      val Yc6 = new Array[Double](InterestVector.Lc6)
      val Yc7 = new Array[Double](InterestVector.Lc7)
      val Yc8 = new Array[Double](InterestVector.Lc8)
      val Yc9 = new Array[Double](InterestVector.Lc9)
      val Yc10 = new Array[Double](InterestVector.Lc10)
      val Yc11 = new Array[Double](InterestVector.Lc11)
      val Yc12 = new Array[Double](InterestVector.Lc12)
      val Yc13 = new Array[Double](InterestVector.Lc13)
      val Yc14 = new Array[Double](InterestVector.Lc14)
      val Yc15 = new Array[Double](InterestVector.Lc15)
      val Yc16 = new Array[Double](InterestVector.Lc16)
      val Yc17 = new Array[Double](InterestVector.Lc17)
      val Yc18 = new Array[Double](InterestVector.Lc18)
      val Yc19 = new Array[Double](InterestVector.Lc19)
      val Yc20 = new Array[Double](InterestVector.Lc20)
      val Yc21 = new Array[Double](InterestVector.Lc21)
      val Yc22 = new Array[Double](InterestVector.Lc22)
      val Yc23 = new Array[Double](InterestVector.Lc23)
      for(i<-0 until InterestVector.Ll1){
        Xl1(i) = a(InterestVector.L1(i))
        Yl1(i) = b(InterestVector.L1(i))
        X(i) = a(InterestVector.L1(i))
        Y(i) = b(InterestVector.L1(i))
      }
      for(i<-0 until InterestVector.Ll2){
        Xl2(i) = a(InterestVector.L2(i))
        Yl2(i) = b(InterestVector.L2(i))
        X(i+InterestVector.Ll1) = a(InterestVector.L2(i))
        Y(i+InterestVector.Ll1) = b(InterestVector.L2(i))
      }
      for(i<-0 until InterestVector.Ll3){
        Xl3(i) = a(InterestVector.L3(i))
        Yl3(i) = b(InterestVector.L3(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2) = a(InterestVector.L3(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2) = b(InterestVector.L3(i))
      }
      for(i<-0 until InterestVector.Ll4){
        Xl4(i) = a(InterestVector.L4(i))
        Yl4(i) = b(InterestVector.L4(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = a(InterestVector.L4(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = b(InterestVector.L4(i))
      }
      for(i<-0 until InterestVector.Lc1){
        Xc1(i) = a(InterestVector.C1(i))
        Yc1(i) = b(InterestVector.C1(i))
      }
      for(i<-0 until InterestVector.Lc2){
        Xc2(i) = a(InterestVector.C2(i))
        Yc2(i) = b(InterestVector.C2(i))
      }
      for(i<-0 until InterestVector.Lc3){
        Xc3(i) = a(InterestVector.C3(i))
        Yc3(i) = b(InterestVector.C3(i))
      }
      for(i<-0 until InterestVector.Lc4){
        Xc4(i) = a(InterestVector.C4(i))
        Yc4(i) = b(InterestVector.C4(i))
      }
      for(i<-0 until InterestVector.Lc5){
        Xc5(i) = a(InterestVector.C5(i))
        Yc5(i) = b(InterestVector.C5(i))
      }
      for(i<-0 until InterestVector.Lc6){
        Xc6(i) = a(InterestVector.C6(i))
        Yc6(i) = b(InterestVector.C6(i))
      }
      for(i<-0 until InterestVector.Lc7){
        Xc7(i) = a(InterestVector.C7(i))
        Yc7(i) = b(InterestVector.C7(i))
      }
      for(i<-0 until InterestVector.Lc8){
        Xc8(i) = a(InterestVector.C8(i))
        Yc8(i) = b(InterestVector.C8(i))
      }
      for(i<-0 until InterestVector.Lc9){
        Xc9(i) = a(InterestVector.C9(i))
        Yc9(i) = b(InterestVector.C9(i))
      }
      for(i<-0 until InterestVector.Lc10){
        Xc10(i) = a(InterestVector.C10(i))
        Yc10(i) = b(InterestVector.C10(i))
      }
      for(i<-0 until InterestVector.Lc11){
        Xc11(i) = a(InterestVector.C11(i))
        Yc11(i) = b(InterestVector.C11(i))
      }
      for(i<-0 until InterestVector.Lc12){
        Xc12(i) = a(InterestVector.C12(i))
        Yc12(i) = b(InterestVector.C12(i))
      }
      for(i<-0 until InterestVector.Lc13){
        Xc13(i) = a(InterestVector.C13(i))
        Yc13(i) = b(InterestVector.C13(i))
      }
      for(i<-0 until InterestVector.Lc14){
        Xc14(i) = a(InterestVector.C14(i))
        Yc14(i) = b(InterestVector.C14(i))
      }
      for(i<-0 until InterestVector.Lc15){
        Xc15(i) = a(InterestVector.C15(i))
        Yc15(i) = b(InterestVector.C15(i))
      }
      for(i<-0 until InterestVector.Lc16){
        Xc16(i) = a(InterestVector.C16(i))
        Yc16(i) = b(InterestVector.C16(i))
      }
      for(i<-0 until InterestVector.Lc17){
        Xc17(i) = a(InterestVector.C17(i))
        Yc17(i) = b(InterestVector.C17(i))
      }
      for(i<-0 until InterestVector.Lc18){
        Xc18(i) = a(InterestVector.C18(i))
        Yc18(i) = b(InterestVector.C18(i))
      }
      for(i<-0 until InterestVector.Lc19){
        Xc19(i) = a(InterestVector.C19(i))
        Yc19(i) = b(InterestVector.C19(i))
      }
      for(i<-0 until InterestVector.Lc20){
        Xc20(i) = a(InterestVector.C20(i))
        Yc20(i) = b(InterestVector.C20(i))
      }
      for(i<-0 until InterestVector.Lc21){
        Xc21(i) = a(InterestVector.C21(i))
        Yc21(i) = b(InterestVector.C21(i))
      }
      for(i<-0 until InterestVector.Lc22){
        Xc22(i) = a(InterestVector.C22(i))
        Yc22(i) = b(InterestVector.C22(i))
      }
      for(i<-0 until InterestVector.Lc23){
        Xc23(i) = a(InterestVector.C23(i))
        Yc23(i) = b(InterestVector.C23(i))
      }
      result(0) = CosineCoefficientSimilarity(X,Y)
      result(1) = CosineCoefficientSimilarity(Xc1,Yc1)
      result(2) = CosineCoefficientSimilarity(Xc2,Yc2)
      result(3) = CosineCoefficientSimilarity(Xc3,Yc3)
      result(4) = CosineCoefficientSimilarity(Xc4,Yc4)
      result(5) = CosineCoefficientSimilarity(Xc5,Yc5)
      result(6) = CosineCoefficientSimilarity(Xc6,Yc6)
      result(7) = CosineCoefficientSimilarity(Xc7,Yc7)
      result(8) = CosineCoefficientSimilarity(Xc8,Yc8)
      result(9) = CosineCoefficientSimilarity(Xc9,Yc9)
      result(10) = CosineCoefficientSimilarity(Xc10,Yc10)
      result(11) = CosineCoefficientSimilarity(Xc11,Yc11)
      result(12) = CosineCoefficientSimilarity(Xc12,Yc12)
      result(14) = CosineCoefficientSimilarity(Xc14,Yc14)
      result(15) = CosineCoefficientSimilarity(Xc15,Yc15)
      result(16) = CosineCoefficientSimilarity(Xc16,Yc16)
      result(17) = CosineCoefficientSimilarity(Xc17,Yc17)
      result(18) = CosineCoefficientSimilarity(Xc18,Yc18)
      result(19) = CosineCoefficientSimilarity(Xc19,Yc19)
      result(20) = CosineCoefficientSimilarity(Xc20,Yc20)
      result(21) = CosineCoefficientSimilarity(Xc21,Yc21)
      result(22) = CosineCoefficientSimilarity(Xc22,Yc22)
      result(23) = CosineCoefficientSimilarity(Xc23,Yc23)
      result(24) = CosineCoefficientSimilarity(Xl1,Yl1)
      result(25) = CosineCoefficientSimilarity(Xl2,Yl2)
      result(26) = CosineCoefficientSimilarity(Xl3,Yl3)
      result(27) = CosineCoefficientSimilarity(Xl4,Yl4)
      result
    }
  }

  /**
   * All+C23+L4 PearsonCorrelationCoefficient
   * @param a;Array[Double] length = 643
   * @param b:Array[Double] length = 643
   * @return Array[Double] length = 28
   */
  def PearsonCorrelationCoefficient4Interest(a:Array[Double],b:Array[Double]):Array[Double]={
    if(a.length!=b.length){
      val result = new Array[Double](28)
      result
    }else {
      val l = a.length
      var ll = 0
      val result = new Array[Double](28)
      result(13) = 0.0
      val X = new Array[Double](InterestVector.Ll)
      val Y = new Array[Double](InterestVector.Ll)
      val Xl1 = new Array[Double](InterestVector.Ll1)
      val Xl2 = new Array[Double](InterestVector.Ll2)
      val Xl3 = new Array[Double](InterestVector.Ll3)
      val Xl4 = new Array[Double](InterestVector.Ll4)
      val Xc1 = new Array[Double](InterestVector.Lc1)
      val Xc2 = new Array[Double](InterestVector.Lc2)
      val Xc3 = new Array[Double](InterestVector.Lc3)
      val Xc4 = new Array[Double](InterestVector.Lc4)
      val Xc5 = new Array[Double](InterestVector.Lc5)
      val Xc6 = new Array[Double](InterestVector.Lc6)
      val Xc7 = new Array[Double](InterestVector.Lc7)
      val Xc8 = new Array[Double](InterestVector.Lc8)
      val Xc9 = new Array[Double](InterestVector.Lc9)
      val Xc10 = new Array[Double](InterestVector.Lc10)
      val Xc11 = new Array[Double](InterestVector.Lc11)
      val Xc12 = new Array[Double](InterestVector.Lc12)
      val Xc13 = new Array[Double](InterestVector.Lc13)
      val Xc14 = new Array[Double](InterestVector.Lc14)
      val Xc15 = new Array[Double](InterestVector.Lc15)
      val Xc16 = new Array[Double](InterestVector.Lc16)
      val Xc17 = new Array[Double](InterestVector.Lc17)
      val Xc18 = new Array[Double](InterestVector.Lc18)
      val Xc19 = new Array[Double](InterestVector.Lc19)
      val Xc20 = new Array[Double](InterestVector.Lc20)
      val Xc21 = new Array[Double](InterestVector.Lc21)
      val Xc22 = new Array[Double](InterestVector.Lc22)
      val Xc23 = new Array[Double](InterestVector.Lc23)
      val Yl1 = new Array[Double](InterestVector.Ll1)
      val Yl2 = new Array[Double](InterestVector.Ll2)
      val Yl3 = new Array[Double](InterestVector.Ll3)
      val Yl4 = new Array[Double](InterestVector.Ll4)
      val Yc1 = new Array[Double](InterestVector.Lc1)
      val Yc2 = new Array[Double](InterestVector.Lc2)
      val Yc3 = new Array[Double](InterestVector.Lc3)
      val Yc4 = new Array[Double](InterestVector.Lc4)
      val Yc5 = new Array[Double](InterestVector.Lc5)
      val Yc6 = new Array[Double](InterestVector.Lc6)
      val Yc7 = new Array[Double](InterestVector.Lc7)
      val Yc8 = new Array[Double](InterestVector.Lc8)
      val Yc9 = new Array[Double](InterestVector.Lc9)
      val Yc10 = new Array[Double](InterestVector.Lc10)
      val Yc11 = new Array[Double](InterestVector.Lc11)
      val Yc12 = new Array[Double](InterestVector.Lc12)
      val Yc13 = new Array[Double](InterestVector.Lc13)
      val Yc14 = new Array[Double](InterestVector.Lc14)
      val Yc15 = new Array[Double](InterestVector.Lc15)
      val Yc16 = new Array[Double](InterestVector.Lc16)
      val Yc17 = new Array[Double](InterestVector.Lc17)
      val Yc18 = new Array[Double](InterestVector.Lc18)
      val Yc19 = new Array[Double](InterestVector.Lc19)
      val Yc20 = new Array[Double](InterestVector.Lc20)
      val Yc21 = new Array[Double](InterestVector.Lc21)
      val Yc22 = new Array[Double](InterestVector.Lc22)
      val Yc23 = new Array[Double](InterestVector.Lc23)
      for(i<-0 until InterestVector.Ll1){
        Xl1(i) = a(InterestVector.L1(i))
        Yl1(i) = b(InterestVector.L1(i))
        X(i) = a(InterestVector.L1(i))
        Y(i) = b(InterestVector.L1(i))
      }
      for(i<-0 until InterestVector.Ll2){
        Xl2(i) = a(InterestVector.L2(i))
        Yl2(i) = b(InterestVector.L2(i))
        X(i+InterestVector.Ll1) = a(InterestVector.L2(i))
        Y(i+InterestVector.Ll1) = b(InterestVector.L2(i))
      }
      for(i<-0 until InterestVector.Ll3){
        Xl3(i) = a(InterestVector.L3(i))
        Yl3(i) = b(InterestVector.L3(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2) = a(InterestVector.L3(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2) = b(InterestVector.L3(i))
      }
      for(i<-0 until InterestVector.Ll4){
        Xl4(i) = a(InterestVector.L4(i))
        Yl4(i) = b(InterestVector.L4(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = a(InterestVector.L4(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = b(InterestVector.L4(i))
      }
      for(i<-0 until InterestVector.Lc1){
        Xc1(i) = a(InterestVector.C1(i))
        Yc1(i) = b(InterestVector.C1(i))
      }
      for(i<-0 until InterestVector.Lc2){
        Xc2(i) = a(InterestVector.C2(i))
        Yc2(i) = b(InterestVector.C2(i))
      }
      for(i<-0 until InterestVector.Lc3){
        Xc3(i) = a(InterestVector.C3(i))
        Yc3(i) = b(InterestVector.C3(i))
      }
      for(i<-0 until InterestVector.Lc4){
        Xc4(i) = a(InterestVector.C4(i))
        Yc4(i) = b(InterestVector.C4(i))
      }
      for(i<-0 until InterestVector.Lc5){
        Xc5(i) = a(InterestVector.C5(i))
        Yc5(i) = b(InterestVector.C5(i))
      }
      for(i<-0 until InterestVector.Lc6){
        Xc6(i) = a(InterestVector.C6(i))
        Yc6(i) = b(InterestVector.C6(i))
      }
      for(i<-0 until InterestVector.Lc7){
        Xc7(i) = a(InterestVector.C7(i))
        Yc7(i) = b(InterestVector.C7(i))
      }
      for(i<-0 until InterestVector.Lc8){
        Xc8(i) = a(InterestVector.C8(i))
        Yc8(i) = b(InterestVector.C8(i))
      }
      for(i<-0 until InterestVector.Lc9){
        Xc9(i) = a(InterestVector.C9(i))
        Yc9(i) = b(InterestVector.C9(i))
      }
      for(i<-0 until InterestVector.Lc10){
        Xc10(i) = a(InterestVector.C10(i))
        Yc10(i) = b(InterestVector.C10(i))
      }
      for(i<-0 until InterestVector.Lc11){
        Xc11(i) = a(InterestVector.C11(i))
        Yc11(i) = b(InterestVector.C11(i))
      }
      for(i<-0 until InterestVector.Lc12){
        Xc12(i) = a(InterestVector.C12(i))
        Yc12(i) = b(InterestVector.C12(i))
      }
      for(i<-0 until InterestVector.Lc13){
        Xc13(i) = a(InterestVector.C13(i))
        Yc13(i) = b(InterestVector.C13(i))
      }
      for(i<-0 until InterestVector.Lc14){
        Xc14(i) = a(InterestVector.C14(i))
        Yc14(i) = b(InterestVector.C14(i))
      }
      for(i<-0 until InterestVector.Lc15){
        Xc15(i) = a(InterestVector.C15(i))
        Yc15(i) = b(InterestVector.C15(i))
      }
      for(i<-0 until InterestVector.Lc16){
        Xc16(i) = a(InterestVector.C16(i))
        Yc16(i) = b(InterestVector.C16(i))
      }
      for(i<-0 until InterestVector.Lc17){
        Xc17(i) = a(InterestVector.C17(i))
        Yc17(i) = b(InterestVector.C17(i))
      }
      for(i<-0 until InterestVector.Lc18){
        Xc18(i) = a(InterestVector.C18(i))
        Yc18(i) = b(InterestVector.C18(i))
      }
      for(i<-0 until InterestVector.Lc19){
        Xc19(i) = a(InterestVector.C19(i))
        Yc19(i) = b(InterestVector.C19(i))
      }
      for(i<-0 until InterestVector.Lc20){
        Xc20(i) = a(InterestVector.C20(i))
        Yc20(i) = b(InterestVector.C20(i))
      }
      for(i<-0 until InterestVector.Lc21){
        Xc21(i) = a(InterestVector.C21(i))
        Yc21(i) = b(InterestVector.C21(i))
      }
      for(i<-0 until InterestVector.Lc22){
        Xc22(i) = a(InterestVector.C22(i))
        Yc22(i) = b(InterestVector.C22(i))
      }
      for(i<-0 until InterestVector.Lc23){
        Xc23(i) = a(InterestVector.C23(i))
        Yc23(i) = b(InterestVector.C23(i))
      }
      result(0) = PearsonCorrelationCoefficient(X,Y)
      result(1) = PearsonCorrelationCoefficient(Xc1,Yc1)
      result(2) = PearsonCorrelationCoefficient(Xc2,Yc2)
      result(3) = PearsonCorrelationCoefficient(Xc3,Yc3)
      result(4) = PearsonCorrelationCoefficient(Xc4,Yc4)
      result(5) = PearsonCorrelationCoefficient(Xc5,Yc5)
      result(6) = PearsonCorrelationCoefficient(Xc6,Yc6)
      result(7) = PearsonCorrelationCoefficient(Xc7,Yc7)
      result(8) = PearsonCorrelationCoefficient(Xc8,Yc8)
      result(9) = PearsonCorrelationCoefficient(Xc9,Yc9)
      result(10) = PearsonCorrelationCoefficient(Xc10,Yc10)
      result(11) = PearsonCorrelationCoefficient(Xc11,Yc11)
      result(12) = PearsonCorrelationCoefficient(Xc12,Yc12)
      result(14) = PearsonCorrelationCoefficient(Xc14,Yc14)
      result(15) = PearsonCorrelationCoefficient(Xc15,Yc15)
      result(16) = PearsonCorrelationCoefficient(Xc16,Yc16)
      result(17) = PearsonCorrelationCoefficient(Xc17,Yc17)
      result(18) = PearsonCorrelationCoefficient(Xc18,Yc18)
      result(19) = PearsonCorrelationCoefficient(Xc19,Yc19)
      result(20) = PearsonCorrelationCoefficient(Xc20,Yc20)
      result(21) = PearsonCorrelationCoefficient(Xc21,Yc21)
      result(22) = PearsonCorrelationCoefficient(Xc22,Yc22)
      result(23) = PearsonCorrelationCoefficient(Xc23,Yc23)
      result(24) = PearsonCorrelationCoefficient(Xl1,Yl1)
      result(25) = PearsonCorrelationCoefficient(Xl2,Yl2)
      result(26) = PearsonCorrelationCoefficient(Xl3,Yl3)
      result(27) = PearsonCorrelationCoefficient(Xl4,Yl4)
      result
    }
  }

  /**
   * All+C23+L4 JaccardCorrelationCoefficient
   * @param a;Array[Double] length = 643
   * @param b:Array[Double] length = 643
   * @return Array[Double] length = 28
   */
  def JaccardCoefficent4Interest(a:Array[Double],b:Array[Double]):Array[Double]={
    if(a.length!=b.length){
      val result = new Array[Double](28)
      result
    }else {
      val l = a.length
      var ll = 0
      val result = new Array[Double](28)
      result(13) = 0.0
      val X = new Array[Double](InterestVector.Ll)
      val Y = new Array[Double](InterestVector.Ll)
      val Xl1 = new Array[Double](InterestVector.Ll1)
      val Xl2 = new Array[Double](InterestVector.Ll2)
      val Xl3 = new Array[Double](InterestVector.Ll3)
      val Xl4 = new Array[Double](InterestVector.Ll4)
      val Xc1 = new Array[Double](InterestVector.Lc1)
      val Xc2 = new Array[Double](InterestVector.Lc2)
      val Xc3 = new Array[Double](InterestVector.Lc3)
      val Xc4 = new Array[Double](InterestVector.Lc4)
      val Xc5 = new Array[Double](InterestVector.Lc5)
      val Xc6 = new Array[Double](InterestVector.Lc6)
      val Xc7 = new Array[Double](InterestVector.Lc7)
      val Xc8 = new Array[Double](InterestVector.Lc8)
      val Xc9 = new Array[Double](InterestVector.Lc9)
      val Xc10 = new Array[Double](InterestVector.Lc10)
      val Xc11 = new Array[Double](InterestVector.Lc11)
      val Xc12 = new Array[Double](InterestVector.Lc12)
      val Xc13 = new Array[Double](InterestVector.Lc13)
      val Xc14 = new Array[Double](InterestVector.Lc14)
      val Xc15 = new Array[Double](InterestVector.Lc15)
      val Xc16 = new Array[Double](InterestVector.Lc16)
      val Xc17 = new Array[Double](InterestVector.Lc17)
      val Xc18 = new Array[Double](InterestVector.Lc18)
      val Xc19 = new Array[Double](InterestVector.Lc19)
      val Xc20 = new Array[Double](InterestVector.Lc20)
      val Xc21 = new Array[Double](InterestVector.Lc21)
      val Xc22 = new Array[Double](InterestVector.Lc22)
      val Xc23 = new Array[Double](InterestVector.Lc23)
      val Yl1 = new Array[Double](InterestVector.Ll1)
      val Yl2 = new Array[Double](InterestVector.Ll2)
      val Yl3 = new Array[Double](InterestVector.Ll3)
      val Yl4 = new Array[Double](InterestVector.Ll4)
      val Yc1 = new Array[Double](InterestVector.Lc1)
      val Yc2 = new Array[Double](InterestVector.Lc2)
      val Yc3 = new Array[Double](InterestVector.Lc3)
      val Yc4 = new Array[Double](InterestVector.Lc4)
      val Yc5 = new Array[Double](InterestVector.Lc5)
      val Yc6 = new Array[Double](InterestVector.Lc6)
      val Yc7 = new Array[Double](InterestVector.Lc7)
      val Yc8 = new Array[Double](InterestVector.Lc8)
      val Yc9 = new Array[Double](InterestVector.Lc9)
      val Yc10 = new Array[Double](InterestVector.Lc10)
      val Yc11 = new Array[Double](InterestVector.Lc11)
      val Yc12 = new Array[Double](InterestVector.Lc12)
      val Yc13 = new Array[Double](InterestVector.Lc13)
      val Yc14 = new Array[Double](InterestVector.Lc14)
      val Yc15 = new Array[Double](InterestVector.Lc15)
      val Yc16 = new Array[Double](InterestVector.Lc16)
      val Yc17 = new Array[Double](InterestVector.Lc17)
      val Yc18 = new Array[Double](InterestVector.Lc18)
      val Yc19 = new Array[Double](InterestVector.Lc19)
      val Yc20 = new Array[Double](InterestVector.Lc20)
      val Yc21 = new Array[Double](InterestVector.Lc21)
      val Yc22 = new Array[Double](InterestVector.Lc22)
      val Yc23 = new Array[Double](InterestVector.Lc23)
      for(i<-0 until InterestVector.Ll1){
        Xl1(i) = a(InterestVector.L1(i))
        Yl1(i) = b(InterestVector.L1(i))
        X(i) = a(InterestVector.L1(i))
        Y(i) = b(InterestVector.L1(i))
      }
      for(i<-0 until InterestVector.Ll2){
        Xl2(i) = a(InterestVector.L2(i))
        Yl2(i) = b(InterestVector.L2(i))
        X(i+InterestVector.Ll1) = a(InterestVector.L2(i))
        Y(i+InterestVector.Ll1) = b(InterestVector.L2(i))
      }
      for(i<-0 until InterestVector.Ll3){
        Xl3(i) = a(InterestVector.L3(i))
        Yl3(i) = b(InterestVector.L3(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2) = a(InterestVector.L3(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2) = b(InterestVector.L3(i))
      }
      for(i<-0 until InterestVector.Ll4){
        Xl4(i) = a(InterestVector.L4(i))
        Yl4(i) = b(InterestVector.L4(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = a(InterestVector.L4(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = b(InterestVector.L4(i))
      }
      for(i<-0 until InterestVector.Lc1){
        Xc1(i) = a(InterestVector.C1(i))
        Yc1(i) = b(InterestVector.C1(i))
      }
      for(i<-0 until InterestVector.Lc2){
        Xc2(i) = a(InterestVector.C2(i))
        Yc2(i) = b(InterestVector.C2(i))
      }
      for(i<-0 until InterestVector.Lc3){
        Xc3(i) = a(InterestVector.C3(i))
        Yc3(i) = b(InterestVector.C3(i))
      }
      for(i<-0 until InterestVector.Lc4){
        Xc4(i) = a(InterestVector.C4(i))
        Yc4(i) = b(InterestVector.C4(i))
      }
      for(i<-0 until InterestVector.Lc5){
        Xc5(i) = a(InterestVector.C5(i))
        Yc5(i) = b(InterestVector.C5(i))
      }
      for(i<-0 until InterestVector.Lc6){
        Xc6(i) = a(InterestVector.C6(i))
        Yc6(i) = b(InterestVector.C6(i))
      }
      for(i<-0 until InterestVector.Lc7){
        Xc7(i) = a(InterestVector.C7(i))
        Yc7(i) = b(InterestVector.C7(i))
      }
      for(i<-0 until InterestVector.Lc8){
        Xc8(i) = a(InterestVector.C8(i))
        Yc8(i) = b(InterestVector.C8(i))
      }
      for(i<-0 until InterestVector.Lc9){
        Xc9(i) = a(InterestVector.C9(i))
        Yc9(i) = b(InterestVector.C9(i))
      }
      for(i<-0 until InterestVector.Lc10){
        Xc10(i) = a(InterestVector.C10(i))
        Yc10(i) = b(InterestVector.C10(i))
      }
      for(i<-0 until InterestVector.Lc11){
        Xc11(i) = a(InterestVector.C11(i))
        Yc11(i) = b(InterestVector.C11(i))
      }
      for(i<-0 until InterestVector.Lc12){
        Xc12(i) = a(InterestVector.C12(i))
        Yc12(i) = b(InterestVector.C12(i))
      }
      for(i<-0 until InterestVector.Lc13){
        Xc13(i) = a(InterestVector.C13(i))
        Yc13(i) = b(InterestVector.C13(i))
      }
      for(i<-0 until InterestVector.Lc14){
        Xc14(i) = a(InterestVector.C14(i))
        Yc14(i) = b(InterestVector.C14(i))
      }
      for(i<-0 until InterestVector.Lc15){
        Xc15(i) = a(InterestVector.C15(i))
        Yc15(i) = b(InterestVector.C15(i))
      }
      for(i<-0 until InterestVector.Lc16){
        Xc16(i) = a(InterestVector.C16(i))
        Yc16(i) = b(InterestVector.C16(i))
      }
      for(i<-0 until InterestVector.Lc17){
        Xc17(i) = a(InterestVector.C17(i))
        Yc17(i) = b(InterestVector.C17(i))
      }
      for(i<-0 until InterestVector.Lc18){
        Xc18(i) = a(InterestVector.C18(i))
        Yc18(i) = b(InterestVector.C18(i))
      }
      for(i<-0 until InterestVector.Lc19){
        Xc19(i) = a(InterestVector.C19(i))
        Yc19(i) = b(InterestVector.C19(i))
      }
      for(i<-0 until InterestVector.Lc20){
        Xc20(i) = a(InterestVector.C20(i))
        Yc20(i) = b(InterestVector.C20(i))
      }
      for(i<-0 until InterestVector.Lc21){
        Xc21(i) = a(InterestVector.C21(i))
        Yc21(i) = b(InterestVector.C21(i))
      }
      for(i<-0 until InterestVector.Lc22){
        Xc22(i) = a(InterestVector.C22(i))
        Yc22(i) = b(InterestVector.C22(i))
      }
      for(i<-0 until InterestVector.Lc23){
        Xc23(i) = a(InterestVector.C23(i))
        Yc23(i) = b(InterestVector.C23(i))
      }
      result(0) = JaccardCoefficent(X,Y)
      result(1) = JaccardCoefficent(Xc1,Yc1)
      result(2) = JaccardCoefficent(Xc2,Yc2)
      result(3) = JaccardCoefficent(Xc3,Yc3)
      result(4) = JaccardCoefficent(Xc4,Yc4)
      result(5) = JaccardCoefficent(Xc5,Yc5)
      result(6) = JaccardCoefficent(Xc6,Yc6)
      result(7) = JaccardCoefficent(Xc7,Yc7)
      result(8) = JaccardCoefficent(Xc8,Yc8)
      result(9) = JaccardCoefficent(Xc9,Yc9)
      result(10) = JaccardCoefficent(Xc10,Yc10)
      result(11) = JaccardCoefficent(Xc11,Yc11)
      result(12) = JaccardCoefficent(Xc12,Yc12)
      result(14) = JaccardCoefficent(Xc14,Yc14)
      result(15) = JaccardCoefficent(Xc15,Yc15)
      result(16) = JaccardCoefficent(Xc16,Yc16)
      result(17) = JaccardCoefficent(Xc17,Yc17)
      result(18) = JaccardCoefficent(Xc18,Yc18)
      result(19) = JaccardCoefficent(Xc19,Yc19)
      result(20) = JaccardCoefficent(Xc20,Yc20)
      result(21) = JaccardCoefficent(Xc21,Yc21)
      result(22) = JaccardCoefficent(Xc22,Yc22)
      result(23) = JaccardCoefficent(Xc23,Yc23)
      result(24) = JaccardCoefficent(Xl1,Yl1)
      result(25) = JaccardCoefficent(Xl2,Yl2)
      result(26) = JaccardCoefficent(Xl3,Yl3)
      result(27) = JaccardCoefficent(Xl4,Yl4)
      result
    }
  }

  /**
   * All+C23+L4 TanimotoCorrelationCoefficient
   * @param a;Array[Double] length = 643
   * @param b:Array[Double] length = 643
   * @return Array[Double] length = 28
   */
  def TanimotoCoefficent4Interest(a:Array[Double],b:Array[Double]):Array[Double]={
    if(a.length!=b.length){
      val result = new Array[Double](28)
      result
    }else {
      val l = a.length
      var ll = 0
      val result = new Array[Double](28)
      result(13) = 0.0
      val X = new Array[Double](InterestVector.Ll)
      val Y = new Array[Double](InterestVector.Ll)
      val Xl1 = new Array[Double](InterestVector.Ll1)
      val Xl2 = new Array[Double](InterestVector.Ll2)
      val Xl3 = new Array[Double](InterestVector.Ll3)
      val Xl4 = new Array[Double](InterestVector.Ll4)
      val Xc1 = new Array[Double](InterestVector.Lc1)
      val Xc2 = new Array[Double](InterestVector.Lc2)
      val Xc3 = new Array[Double](InterestVector.Lc3)
      val Xc4 = new Array[Double](InterestVector.Lc4)
      val Xc5 = new Array[Double](InterestVector.Lc5)
      val Xc6 = new Array[Double](InterestVector.Lc6)
      val Xc7 = new Array[Double](InterestVector.Lc7)
      val Xc8 = new Array[Double](InterestVector.Lc8)
      val Xc9 = new Array[Double](InterestVector.Lc9)
      val Xc10 = new Array[Double](InterestVector.Lc10)
      val Xc11 = new Array[Double](InterestVector.Lc11)
      val Xc12 = new Array[Double](InterestVector.Lc12)
      val Xc13 = new Array[Double](InterestVector.Lc13)
      val Xc14 = new Array[Double](InterestVector.Lc14)
      val Xc15 = new Array[Double](InterestVector.Lc15)
      val Xc16 = new Array[Double](InterestVector.Lc16)
      val Xc17 = new Array[Double](InterestVector.Lc17)
      val Xc18 = new Array[Double](InterestVector.Lc18)
      val Xc19 = new Array[Double](InterestVector.Lc19)
      val Xc20 = new Array[Double](InterestVector.Lc20)
      val Xc21 = new Array[Double](InterestVector.Lc21)
      val Xc22 = new Array[Double](InterestVector.Lc22)
      val Xc23 = new Array[Double](InterestVector.Lc23)
      val Yl1 = new Array[Double](InterestVector.Ll1)
      val Yl2 = new Array[Double](InterestVector.Ll2)
      val Yl3 = new Array[Double](InterestVector.Ll3)
      val Yl4 = new Array[Double](InterestVector.Ll4)
      val Yc1 = new Array[Double](InterestVector.Lc1)
      val Yc2 = new Array[Double](InterestVector.Lc2)
      val Yc3 = new Array[Double](InterestVector.Lc3)
      val Yc4 = new Array[Double](InterestVector.Lc4)
      val Yc5 = new Array[Double](InterestVector.Lc5)
      val Yc6 = new Array[Double](InterestVector.Lc6)
      val Yc7 = new Array[Double](InterestVector.Lc7)
      val Yc8 = new Array[Double](InterestVector.Lc8)
      val Yc9 = new Array[Double](InterestVector.Lc9)
      val Yc10 = new Array[Double](InterestVector.Lc10)
      val Yc11 = new Array[Double](InterestVector.Lc11)
      val Yc12 = new Array[Double](InterestVector.Lc12)
      val Yc13 = new Array[Double](InterestVector.Lc13)
      val Yc14 = new Array[Double](InterestVector.Lc14)
      val Yc15 = new Array[Double](InterestVector.Lc15)
      val Yc16 = new Array[Double](InterestVector.Lc16)
      val Yc17 = new Array[Double](InterestVector.Lc17)
      val Yc18 = new Array[Double](InterestVector.Lc18)
      val Yc19 = new Array[Double](InterestVector.Lc19)
      val Yc20 = new Array[Double](InterestVector.Lc20)
      val Yc21 = new Array[Double](InterestVector.Lc21)
      val Yc22 = new Array[Double](InterestVector.Lc22)
      val Yc23 = new Array[Double](InterestVector.Lc23)
      for(i<-0 until InterestVector.Ll1){
        Xl1(i) = a(InterestVector.L1(i))
        Yl1(i) = b(InterestVector.L1(i))
        X(i) = a(InterestVector.L1(i))
        Y(i) = b(InterestVector.L1(i))
      }
      for(i<-0 until InterestVector.Ll2){
        Xl2(i) = a(InterestVector.L2(i))
        Yl2(i) = b(InterestVector.L2(i))
        X(i+InterestVector.Ll1) = a(InterestVector.L2(i))
        Y(i+InterestVector.Ll1) = b(InterestVector.L2(i))
      }
      for(i<-0 until InterestVector.Ll3){
        Xl3(i) = a(InterestVector.L3(i))
        Yl3(i) = b(InterestVector.L3(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2) = a(InterestVector.L3(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2) = b(InterestVector.L3(i))
      }
      for(i<-0 until InterestVector.Ll4){
        Xl4(i) = a(InterestVector.L4(i))
        Yl4(i) = b(InterestVector.L4(i))
        X(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = a(InterestVector.L4(i))
        Y(i+InterestVector.Ll1+InterestVector.Ll2+InterestVector.Ll3) = b(InterestVector.L4(i))
      }
      for(i<-0 until InterestVector.Lc1){
        Xc1(i) = a(InterestVector.C1(i))
        Yc1(i) = b(InterestVector.C1(i))
      }
      for(i<-0 until InterestVector.Lc2){
        Xc2(i) = a(InterestVector.C2(i))
        Yc2(i) = b(InterestVector.C2(i))
      }
      for(i<-0 until InterestVector.Lc3){
        Xc3(i) = a(InterestVector.C3(i))
        Yc3(i) = b(InterestVector.C3(i))
      }
      for(i<-0 until InterestVector.Lc4){
        Xc4(i) = a(InterestVector.C4(i))
        Yc4(i) = b(InterestVector.C4(i))
      }
      for(i<-0 until InterestVector.Lc5){
        Xc5(i) = a(InterestVector.C5(i))
        Yc5(i) = b(InterestVector.C5(i))
      }
      for(i<-0 until InterestVector.Lc6){
        Xc6(i) = a(InterestVector.C6(i))
        Yc6(i) = b(InterestVector.C6(i))
      }
      for(i<-0 until InterestVector.Lc7){
        Xc7(i) = a(InterestVector.C7(i))
        Yc7(i) = b(InterestVector.C7(i))
      }
      for(i<-0 until InterestVector.Lc8){
        Xc8(i) = a(InterestVector.C8(i))
        Yc8(i) = b(InterestVector.C8(i))
      }
      for(i<-0 until InterestVector.Lc9){
        Xc9(i) = a(InterestVector.C9(i))
        Yc9(i) = b(InterestVector.C9(i))
      }
      for(i<-0 until InterestVector.Lc10){
        Xc10(i) = a(InterestVector.C10(i))
        Yc10(i) = b(InterestVector.C10(i))
      }
      for(i<-0 until InterestVector.Lc11){
        Xc11(i) = a(InterestVector.C11(i))
        Yc11(i) = b(InterestVector.C11(i))
      }
      for(i<-0 until InterestVector.Lc12){
        Xc12(i) = a(InterestVector.C12(i))
        Yc12(i) = b(InterestVector.C12(i))
      }
      for(i<-0 until InterestVector.Lc13){
        Xc13(i) = a(InterestVector.C13(i))
        Yc13(i) = b(InterestVector.C13(i))
      }
      for(i<-0 until InterestVector.Lc14){
        Xc14(i) = a(InterestVector.C14(i))
        Yc14(i) = b(InterestVector.C14(i))
      }
      for(i<-0 until InterestVector.Lc15){
        Xc15(i) = a(InterestVector.C15(i))
        Yc15(i) = b(InterestVector.C15(i))
      }
      for(i<-0 until InterestVector.Lc16){
        Xc16(i) = a(InterestVector.C16(i))
        Yc16(i) = b(InterestVector.C16(i))
      }
      for(i<-0 until InterestVector.Lc17){
        Xc17(i) = a(InterestVector.C17(i))
        Yc17(i) = b(InterestVector.C17(i))
      }
      for(i<-0 until InterestVector.Lc18){
        Xc18(i) = a(InterestVector.C18(i))
        Yc18(i) = b(InterestVector.C18(i))
      }
      for(i<-0 until InterestVector.Lc19){
        Xc19(i) = a(InterestVector.C19(i))
        Yc19(i) = b(InterestVector.C19(i))
      }
      for(i<-0 until InterestVector.Lc20){
        Xc20(i) = a(InterestVector.C20(i))
        Yc20(i) = b(InterestVector.C20(i))
      }
      for(i<-0 until InterestVector.Lc21){
        Xc21(i) = a(InterestVector.C21(i))
        Yc21(i) = b(InterestVector.C21(i))
      }
      for(i<-0 until InterestVector.Lc22){
        Xc22(i) = a(InterestVector.C22(i))
        Yc22(i) = b(InterestVector.C22(i))
      }
      for(i<-0 until InterestVector.Lc23){
        Xc23(i) = a(InterestVector.C23(i))
        Yc23(i) = b(InterestVector.C23(i))
      }
      result(0) = TanimotoCoefficent(X,Y)
      result(1) = TanimotoCoefficent(Xc1,Yc1)
      result(2) = TanimotoCoefficent(Xc2,Yc2)
      result(3) = TanimotoCoefficent(Xc3,Yc3)
      result(4) = TanimotoCoefficent(Xc4,Yc4)
      result(5) = TanimotoCoefficent(Xc5,Yc5)
      result(6) = TanimotoCoefficent(Xc6,Yc6)
      result(7) = TanimotoCoefficent(Xc7,Yc7)
      result(8) = TanimotoCoefficent(Xc8,Yc8)
      result(9) = TanimotoCoefficent(Xc9,Yc9)
      result(10) = TanimotoCoefficent(Xc10,Yc10)
      result(11) = TanimotoCoefficent(Xc11,Yc11)
      result(12) = TanimotoCoefficent(Xc12,Yc12)
      result(14) = TanimotoCoefficent(Xc14,Yc14)
      result(15) = TanimotoCoefficent(Xc15,Yc15)
      result(16) = TanimotoCoefficent(Xc16,Yc16)
      result(17) = TanimotoCoefficent(Xc17,Yc17)
      result(18) = TanimotoCoefficent(Xc18,Yc18)
      result(19) = TanimotoCoefficent(Xc19,Yc19)
      result(20) = TanimotoCoefficent(Xc20,Yc20)
      result(21) = TanimotoCoefficent(Xc21,Yc21)
      result(22) = TanimotoCoefficent(Xc22,Yc22)
      result(23) = TanimotoCoefficent(Xc23,Yc23)
      result(24) = TanimotoCoefficent(Xl1,Yl1)
      result(25) = TanimotoCoefficent(Xl2,Yl2)
      result(26) = TanimotoCoefficent(Xl3,Yl3)
      result(27) = TanimotoCoefficent(Xl4,Yl4)
      result
    }
  }

  /**
   * EuclideanDistance
   * @param a:Array[Double]
   * @param b:Array[Double]
   * @return Double
   */
  def EuclideanDistance(a:Array[Double],b:Array[Double]):Double={
    if(a.length!=b.length){
      0.0
    }else{
      val l = a.length
      var dis = 0.0
      for(i<-0 until l){
        dis = dis + (a(i)-b(i))*(a(i)-b(i))
      }
      sqrt(dis)
    }
  }

  /**
   * ManhattanDistance
   * @param a:Array[Double]
   * @param b:Array[Double]
   * @return Double
   */
  def ManhattanDistance(a:Array[Double],b:Array[Double]):Double={
    if(a.length!=b.length){
      0.0
    }else{
      val l = a.length
      var dis = 0.0
      for(i<-0 until l){
        dis = dis + max(a(i),b(i))-min(a(i),b(i))
      }
      dis
    }
  }

  /**
   * get opposite number
   * @param d:Double
   * @return Double
   */
  def negetiveTransform(d:Double):Double={
    val s = -d
    s
  }

  /**
   * get reciprocal
   * @param d:Double
   * @return Double
   */
  def ZeroOneTransform(d:Double):Double={
    val s = 1/(1+d)
    s
  }

  /**
   * get e-d
   * @param d:Double
   * @return Double
   */
  def naturalTransform(d:Double):Double={
    val s = math.exp(-d)
    s
  }
}

