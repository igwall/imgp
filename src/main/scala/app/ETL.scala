object ETL {

  // See if we need to use string or int
  def cleanOs(os: String) {
    val osLower = os.toLowerCase()

    if (osLower.contains("win")) "windows"
    else if (osLower.contains("ios")) "ios"
    else if (osLower.contains("android")) "android"
    else if (osLower.contains("rim")) "rim"
    else "other"
  }
}
