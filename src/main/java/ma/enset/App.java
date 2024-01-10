package ma.enset;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class App {
    public static void main(String[] args) {

        SparkSession ss = SparkSession.builder().appName("Exam_BigData").master("local[*]").getOrCreate();
        Dataset<Row> df1 = ss.read().option("header", true).option("inferSchema", true).csv("achat.csv");
        df1.show();
        df1.printSchema();

        //Question1:1. Afficher le produit le plus vendu en terme de montant total.
        df1.groupBy(col("produit_id")).sum("montant").orderBy(col("sum(montant)").desc()).limit(1).show();
        //Question2:2. Afficher les 3 produits les plus vendus dans l'ensemble des données.
        df1.groupBy(col("produit_id")).sum("montant").orderBy(col("sum(montant)").desc()).limit(3).show();
        //Question3: Afficher le montant total des achats pour chaque produit.
        df1.groupBy(col("produit_id")).sum("montant").show();
    }
}