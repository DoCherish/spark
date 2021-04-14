import org.apache.commons.codec.digest.DigestUtils

/**
 * @Author Do
 * @Date 2021/4/14 10:44
 */
object test {

  def main(args: Array[String]): Unit = {


    // enterprise_id
    // train_id
    // user_id
    // personRK: String = DigestUtils.md5Hex(enterpriseId + "_" + trainId + "_" + userId)
    print(DigestUtils.md5Hex("e2_" + "t2_" +"u2"))
    // 6fa766afced1f5420327d7ac4fcf2848
    // 5bbe0f922ec1740ca9fba9cc3a716641
  }

}
