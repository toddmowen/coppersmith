package commbank.coppersmith

package scalding

import com.twitter.scalding.typed._

import org.scalacheck.Prop.forAll
import shapeless.Generic

import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.std.option.ToOptionIdOps

import au.com.cba.omnia.thermometer.core.ThermometerSpec

import commbank.coppersmith._, SourceBinder._
import Arbitraries._
import test.thrift.{Account, Customer}

import lift.ScaldingScalazInstances._

object ScaldingFeatureSourceSpec extends ThermometerSpec { def is = s2"""
  SelectFeatureSet - Test an example set of features based on selecting fields
  ===========
  A From feature source
    must apply filter $fromFilter ${tag("slow")}

  A Join feature source
    must apply filter $joinFilter ${tag("slow")}

  A LeftJoin feature source
    must apply filter $leftJoinFilter ${tag("slow")}

  A multiway join feature source
    must have correct results $multiwayJoin  ${tag("slow")}
    must apply filter $multiwayJoin  ${tag("slow")}
"""

  // FIXME: Pull up to test project for use by client apps
  case class TestDataSource[T](testData: Iterable[T]) extends DataSource[T, TypedPipe] {
    def load = IterablePipe(testData)
  }

  def fromFilter = forAll { (cs: List[Customer]) => {
    val filter = (c: Customer) => c.age < 18
    val source = From[Customer].filter(filter)

    val loaded = source.bind(from(TestDataSource(cs))).load
    runsSuccessfully(loaded) must_== cs.filter(filter)
  }}.set(minTestsOk = 10)

  def joinFilter = forAll { (customerAccounts: CustomerAccounts) => {
    val cas = customerAccounts.cas
    def filter(ca: (Customer, Account)) = ca._1.age < 18

    val expected = cas.flatMap(ca => ca.as.map(a => (ca.c, a))).filter(filter)

    val source = Join[Customer].to[Account].on(_.id, _.customerId).filter(filter)
    val bound = source.bind(join(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))))
    runsSuccessfully(bound.load).toSet must_== expected.toSet
  }}.set(minTestsOk = 10)

  def leftJoinFilter = forAll { (customerAccounts: CustomerAccounts) => {
    val cas = customerAccounts.cas
    def filter(ca: (Customer, Option[Account])) = ca._1.age < 18

    val expected = cas.flatMap(ca =>
      ca.as.toList.toNel.map(as =>
        as.list.map(a => (ca.c, a.some))
      ).getOrElse(List((ca.c, None)))
    ).filter(filter)

    val source = Join.left[Customer].to[Account].on(_.id, _.customerId).filter(filter)
    val bound = source.bind(leftJoin(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))))
    runsSuccessfully(bound.load).toSet must_== expected.toSet
  }}.set(minTestsOk = 10)


  def multiwayJoin = forAll { (customerAccounts: CustomerAccounts) => {
    //shadow accidentally imported implicit
    implicit val genMonad = 1

    val cas = customerAccounts.cas
    def filter(ca: (Customer, Account)) = ca._1.age < 18

    val expected = cas.flatMap(ca => ca.as.map(a => (ca.c, a)))

    val source = Join.multiway[Customer].inner[Account] .on((c: Customer) => c.id, (a: Account) => a.customerId)


    val customersDs: DataSource[Customer, TypedPipe] = TestDataSource(cas.map(_.c))
    val accountsDs: DataSource[Account, TypedPipe] = TestDataSource(cas.flatMap(_.as))

    val bound = joinMulti((customersDs, accountsDs), source).bind(())
    runsSuccessfully(bound).toSet must_== expected.toSet

  }}.set(minTestsOk = 10)

//  def multiwayFilter =  {
//    def filter(ca: (Customer, Account)) = ca._1.age < 18
//
//
//    val source = Join.multiway[Customer].inner[Account] .on((c: Customer) => c.id, (a: Account) => a.customerId)
//
//
//    val customers: Seq[Customer] = Seq()
//    val accounts : Seq[Account] = Seq()
//
//    val customersDs: DataSource[Customer, TypedPipe] = TestDataSource(customers)
//    val accountsDs: DataSource[Account, TypedPipe] = TestDataSource(accounts)
//
//    //a dirty hack to shadow the monad instance brought in by thermometer
//    implicit val genMonad = 1
//    val bound = joinMulti((customersDs, accountsDs), source).bind(())
//
//    runsSuccessfully(bound).toSet must_== Set()
//  }
}
