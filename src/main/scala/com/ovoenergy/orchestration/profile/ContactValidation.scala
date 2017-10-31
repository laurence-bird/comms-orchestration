package com.ovoenergy.orchestration.profile

import com.ovoenergy.comms.model.{CustomerAddress, InvalidProfile}
import com.ovoenergy.orchestration.domain
import com.ovoenergy.orchestration.domain.MobilePhoneNumber
import com.ovoenergy.orchestration.processes.Orchestrator.ErrorDetails

object ContactValidation {

  private val validUkMobileRegex = """^\+447\d{9}$""".r
  private val phoneNumberPrefix = """^(07|447|0{1,2}447)"""

  def validateMobileNumber(number: String): Either[ErrorDetails, MobilePhoneNumber] = {
    val strippedPhoneNumber = number.trim
      .replaceAll("[^0-9]", "")
      .replaceFirst(phoneNumberPrefix, """+447""")
    validUkMobileRegex
      .findFirstIn(strippedPhoneNumber)
      .map(MobilePhoneNumber(_))
      .toRight(ErrorDetails("Invalid phone number provided", InvalidProfile))
  }

  def validatePostalAddress(address: domain.ContactAddress): Either[ErrorDetails, domain.ContactAddress] = {
    AddressValidator
      .validateAddress(address)
      .toEither
      .left
      .map(errors => ErrorDetails(errors.toList.mkString(", "), InvalidProfile))
  }

}