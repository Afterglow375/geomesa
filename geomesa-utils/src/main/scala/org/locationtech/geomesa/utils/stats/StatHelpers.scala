package org.locationtech.geomesa.utils.stats

import org.joda.time.format.DateTimeFormat

object StatHelpers {
  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
}