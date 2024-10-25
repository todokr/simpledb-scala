package simpledb.filemanagement

/** Identifies a specific block by its file name and logical block number */
final case class BlockId(fileName: String, blockNum: Int) {
  override def toString: String = s"$fileName.$blockNum"
}
