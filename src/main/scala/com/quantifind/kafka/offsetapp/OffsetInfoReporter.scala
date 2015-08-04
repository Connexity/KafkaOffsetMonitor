package com.quantifind.kafka.offsetapp

import com.quantifind.kafka.OffsetGetter
import OffsetGetter.OffsetInfo

trait OffsetInfoReporter {
  def report(info: IndexedSeq[OffsetInfo])
  def cleanupOldData() = {}
}
