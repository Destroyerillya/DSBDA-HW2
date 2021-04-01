package ru.mephi.spark.data;
import scala.Tuple2;
import scala.Array;
import java.util.*;

/**
 * # Программа должна подсчитывать почасовое количество вылетов из страны в страну,
 * # основываясь на информации о рейсах, предоставляемых аэропортами.
 * # Входные данные: справочник Аэропорт-Страна, данные аэропорта в формате:
 * # Номер рейса, время вылета, аэропорт назначения.
 * # Выходные данные: Время (почасовая агрегация, Страна вылета, Страна прилета, количество рейсов.
 * 1908,6:44:35.701139,DMD-Ukraine,HF-Switzerland
 */
public class Util {
    public static List<String> parseLog(Tuple2<String, String> log) {
        List<String> result = new ArrayList<String>();
        List<String> result_strks = new ArrayList<String>();
        List<Integer> result_integers = new ArrayList<Integer>();
        String[] strks = log._2.split("\n");
        int index = 0;
        for (String value: strks) {
            String[] splitLog = (value).split(",");
            String logtime = (splitLog[1].split(":")[0]) + ":00:00";
            String filepath = ((log._1.split("-"))[(log._1.split("-").length) - 1]);
            String srccountry = filepath.split("\\.")[0];
            String dstcountry = (splitLog[2].split("-"))[1];
            if (result_strks.contains(new String(logtime + ", " + srccountry + ", " + dstcountry))){
                for (int i = 0; i < result_strks.size(); i++)
                {
                    String auction = result_strks.get(i);
                    if (auction.equals(new String(logtime + ", " + srccountry + ", " + dstcountry)))
                    {
                        index = i;
                    }
                }
                result_integers.set(index, result_integers.get(index) + 1);
            }
            else {
                result_strks.add(new String(logtime + ", " + srccountry + ", " + dstcountry));
                result_integers.add(new Integer(1));
            }
        }
        for (int i = 0; i < result_strks.size(); i++)
        {
            String auction = result_strks.get(i);
            result.add(new String(auction + ',' + Integer.toString(result_integers.get(i))));
        }
        return result;
    };
}
