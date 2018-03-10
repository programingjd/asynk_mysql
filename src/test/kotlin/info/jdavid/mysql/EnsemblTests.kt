package info.jdavid.mysql

import info.jdavid.sql.use
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Assert.*
import org.junit.Test
import java.net.InetSocketAddress

// Ensembl public SQL Servers
// https;//www.ensembl.org
// https://www.ensembl.org/info/data/mysql.html
class EnsemblTests {

  private val credentials =
    MysqlAuthentication.Credentials.PasswordCredentials("anonymous", "")

  @Test
  fun testMysql() {
    val address = InetSocketAddress("ensembldb.ensembl.org", 3306)
    val databaseName = "aedes_aegypti_core_48_1b"
    runBlocking {
      credentials.connectTo(databaseName, address).use {
        it.rows(
          """
            SELECT
              gene_id,
              biotype,
              analysis_id,
              seq_region_strand,
              description,
              is_current
            FROM gene ORDER BY gene_id LIMIT 1000
          """.trimIndent()
        ).toList().apply {
          assertEquals(1000, size)

          for (i in 1..10) {
            assertEquals(i.toLong(), get(i-1)["gene_id"])
            assertEquals("protein_coding", get(i-1)["biotype"])
            assertEquals(1, get(i-1)["analysis_id"])
            assertEquals(true, get(i-1)["is_current"])
          }

          assertEquals(1.toByte(), get(0)["seq_region_strand"])
          assertEquals(1.toByte(), get(1)["seq_region_strand"])
          assertEquals(1.toByte(), get(2)["seq_region_strand"])
          assertEquals(1.toByte(), get(3)["seq_region_strand"])
          assertEquals(1.toByte(), get(4)["seq_region_strand"])
          assertEquals(1.toByte(), get(5)["seq_region_strand"])
          assertEquals((-1).toByte(), get(6)["seq_region_strand"])
          assertEquals(1.toByte(), get(7)["seq_region_strand"])
          assertEquals((-1).toByte(), get(8)["seq_region_strand"])
          assertEquals((-1).toByte(), get(9)["seq_region_strand"])

          assertEquals("low molecular weight protein-tyrosine-phosphatase", get(0)["description"])
          assertEquals("conserved hypothetical protein", get(1)["description"])
          assertEquals("phosphatidylserine decarboxylase", get(2)["description"])
          assertEquals("60S ribosomal protein L23", get(3)["description"])
          assertEquals("heat shock protein", get(4)["description"])
          assertEquals("organic cation transporter", get(5)["description"])
          assertEquals("lava lamp protein", get(6)["description"])
          assertEquals("hypothetical protein", get(7)["description"])
          assertEquals("lkb1 interacting protein", get(8)["description"])
          assertEquals("NADH:ubiquinone dehydrogenase, putative", get(9)["description"])

        }
      }
    }
  }

  @Test
  fun testMariadb() {
    val address = InetSocketAddress("martdb.ensembl.org", 5316)
    val databaseName = "compara_mart_homology_48"
    runBlocking {
      credentials.connectTo(databaseName, address, 16777215).use {
        it.rows(
          """
            SELECT
              homol_description,
              aaeg1_transcript_id_key,
              aaeg1_description,
              aaeg1_gene_id,
              aaeg1_gene_stable_id_v,
              aaeg1_chrom_strand,
              dn_ds
            FROM compara_aaeg_aaeg_paralogs__paralogs__main
            ORDER BY aaeg1_gene_id, aaeg1_cigar_line LIMIT 1000
          """.trimIndent()
        ).toList().apply {
          assertEquals(1000, size)

          for (i in 0..9) {
            assertEquals("within_species_paralog", get(i)["homol_description"])
          }
          for (i in 0..1) {
            assertEquals(1, get(i)["aaeg1_transcript_id_key"])
            assertEquals(1.toLong(), get(i)["aaeg1_gene_id"])
            assertEquals(1.toByte(), get(i)["aaeg1_chrom_strand"])
            assertEquals("low molecular weight protein-tyrosine-phosphatase", get(i)["aaeg1_description"])
            assertNull(get(i)["dn_ds"])
            assertEquals("AAEL003237.1", get(i)["aaeg1_gene_stable_id_v"])
          }
          for (i in 2..3) {
            assertEquals(2, get(i)["aaeg1_transcript_id_key"])
            assertEquals(2.toLong(), get(i)["aaeg1_gene_id"])
            assertEquals(1.toByte(), get(i)["aaeg1_chrom_strand"])
            assertEquals("conserved hypothetical protein", get(i)["aaeg1_description"])
            assertNull(get(i)["dn_ds"])
            assertEquals("AAEL014602.1", get(i)["aaeg1_gene_stable_id_v"])
          }
          for (i in 4..5) {
            assertEquals(4, get(i)["aaeg1_transcript_id_key"])
            assertEquals(4.toLong(), get(i)["aaeg1_gene_id"])
            assertEquals(1.toByte(), get(i)["aaeg1_chrom_strand"])
            assertEquals("60S ribosomal protein L23", get(i)["aaeg1_description"])
            assertNull(get(i)["dn_ds"])
            assertEquals("AAEL013583.1", get(i)["aaeg1_gene_stable_id_v"])
          }
          for (i in 6..9) {
            assertEquals(5, get(i)["aaeg1_transcript_id_key"])
            assertEquals(5.toLong(), get(i)["aaeg1_gene_id"])
            assertEquals(1.toByte(), get(i)["aaeg1_chrom_strand"])
            assertEquals("heat shock protein", get(i)["aaeg1_description"])
            assertNull(get(i)["dn_ds"])
            assertEquals("AAEL011704.1", get(i)["aaeg1_gene_stable_id_v"])
          }
        }
      }
    }
  }

}
