/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.cargadatos;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author user
 * CREATE TABLE `cliente` ( `ID_CLIENTE` VARCHAR(15), `NOMBRE`
 * VARCHAR(60), `CORREO` VARCHAR(60), `DEUDA` DECIMAL(20), `ID_DEUDA`
 * VARCHAR(15), `FECHA_VENCE` DATE );
 */
public class Cargador {

    private static Logger logger = Logger.getLogger(Cargador.class.getName());
   
    private static String datereg = "^([0-2][0-9]||3[0-1])-(0[0-9]||1[0-2])-([0-9][0-9])?[0-9][0-9]$";
    private static String correoreg = "^(.+)@(.+)$";

    public static void main(String[] args) {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        int insertcount = 0;
        int totalcount = 0;
        int  errcount =0;
        try {
            if ( args.length == 0 ) {
                System.out.println("Debe especificar la ruta del archivo a cargar como parametro a este ejecutable");
                System.exit(1);
            } 
            String csvFile = (args[0]);  
            //String csvFile = "C:\\tmp\\Datos.prn";
            File inputf = new File(csvFile);
            if (!inputf.exists()) {
                System.out.println("No se encontro el archivo " + csvFile);
                System.exit(2);
            }

            String url = "jdbc:mysql://127.0.0.1:3306/prueba";
            String user = "root";
            String password = "root";
       
            String values = "";
            String query = "";
            // try {

            CSVParser csvParser = new CSVParserBuilder().withSeparator(';').build(); // custom separator
            CSVReader reader = new CSVReaderBuilder(new FileReader(csvFile))
                    .withCSVParser(csvParser) // custom CSV parser
                    .build();
            String[] nextLine;

            con = DriverManager.getConnection(url, user, password);
            st = con.createStatement();

            while ((nextLine = reader.readNext()) != null) {
                totalcount++;
                try { 
                    Long.parseLong(nextLine[3]);  //validar numero en la deuda
                    boolean valid  = true;
                    if ( nextLine[0].length() == 0  ){
                        errcount++;
                        logger.log(Level.SEVERE, "Error id_cliente no puede ser vacio, Registro Nº " + totalcount);
                        valid = false;
                    }  
                    if ( nextLine[1].length() == 0  ){
                        errcount++;
                        logger.log(Level.SEVERE, "Error nombre de cliente no puede ser vacio, Registro Nº " + totalcount);
                        valid = false;
                    }
                    if ( nextLine[2].length() == 0  ){
                        errcount++;
                        logger.log(Level.SEVERE, "Error correo de cliente no puede ser vacio, Registro Nº " + totalcount);
                        valid = false;
                    }
                    
                    String sfecha = nextLine[5];
                    if ( !sfecha.matches(datereg) ) {
                        errcount++;
                        logger.log(Level.SEVERE, "Error Formato de fecha no es dd-mm-yyyy, Registro Nº " + totalcount);
                        valid = false;
                    }
                    if ( !nextLine[2].matches(correoreg) ){
                        errcount++;
                        logger.log(Level.SEVERE, "Error Formato correo invalido, Registro Nº " + totalcount);
                        valid = false;
                    }
                    if ( valid ) { 
                        values = " ('" + nextLine[0] + "','" + nextLine[1] + "','" + nextLine[2] + "','" + nextLine[3] + "','" + nextLine[4] + "',  STR_TO_DATE('" + nextLine[5] + "', '%d-%m-%Y'))";
                    
                        query = "INSERT INTO  CLIENTE ("
                            + "ID_CLIENTE ,"
                            + "NOMBRE ,"
                            + "CORREO,"
                            + "DEUDA ,"
                            + "ID_DEUDA ,"
                            + "FECHA_VENCE )"
                            + "VALUES " + values + ";";
                        logger.info("Ejecutando " + query);
                         st.executeUpdate(query);
                        insertcount++;
                    }
                }
                catch ( NumberFormatException n ) {
                    errcount++;
                    logger.log(Level.SEVERE, "Error de formato de numero para valor de deuda en registro " + totalcount);
                }
               
              
            }
            logger.info("Registros leidos " + totalcount);
            logger.info("Registros con error " + errcount);            
            logger.info("Registros grabados " + insertcount);            


        } catch (SQLException ex) {
            errcount++;
            logger.log(Level.SEVERE, ex.getMessage(), ex);
        } catch (Exception e) {
             errcount++;
            e.printStackTrace();
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException ex) {

                logger.log(Level.WARNING, ex.getMessage(), ex);
            }
        }

        //System.out.println(totalcount);
    }
}
