package br.com.imdb.assembler

import com.google.gson.Gson
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*
import java.io.File
import java.net.URL


class MovieProcessor {
    private val spark: SparkSession = SparkSession.builder().appName("IMDb Movie Processing").orCreate

    fun processMovies(movieIds: List<String>? = null): List<File> {
//        val movies = movieIds?.map { id -> getMovie(id) } ?: getAllMovies()
        val movies = getAllMovies()

        //Converte lista de objeto para dataframe
        val schema = StructType(
            listOf(
                StructField("title", DataTypes.StringType, true, Metadata.empty()),
                StructField("year", DataTypes.IntegerType, true, Metadata.empty())
            ).toTypedArray()
        )

//        val rdd = spark.sparkContext().parallelize(movies)
        val rdd = spark.sparkContext().parallelize(movies)
        val rowRDD = rdd.map { movie -> Row(movie.title, movie.year) }
//        val rowRDD = rdd.toDF(movies)
        val movieDF = spark.createDataFrame(rowRDD, schema)

        // Processa os dados utilizando o Spark (filtra, agrupa, etc.)
        val filteredMovies = movieDF.filter("year >= 1960")

        val files = mutableListOf<File>()
        // Salva cada filme em .txt
        filteredMovies.foreach { row ->
            val title = row.getAs<String>("title")
            val year = row.getAs<Int>("year")
            val fileName = "${title}_${year}.txt"
            val fileContent = row.toString()

            // Salva o conteúdo do arquivo na pasta de destino
            val file = File("src/main/resources/temp/$fileName")
            file.writeText(fileContent)
            files.add(file)
        }
        spark.stop()
        return files
    }

    private fun getAllMovies(): List<Movie> {
        val url = URL("https://imdb-api.com/en/API/Top250Movies/k_vmjo2iib")
        val json = url.readText()
        return Gson().fromJson(json, MovieResults::class.java).data.map { it.toMovie() }
    }

    private fun getMovie(id: String): Movie {
        val url = URL("https://imdb-api.com/en/API/Ratings/k_vmjo2iib/$id")
        val json = url.readText()
        return Gson().fromJson(json, MovieResults::class.java).data[0].toMovie()
    }
}