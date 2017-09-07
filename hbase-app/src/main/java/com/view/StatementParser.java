package com.view;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sayed on 30.08.17.
 */
public class StatementParser {

    // s.matches("regex"): Evaluates if "regex" matches s. Returns only true if the WHOLE string can be matched.
    // s.split("regex")  : Creates an array with substrings of s divided at occurrence of "regex".
    //                     "regex" is not included in the result.
    // s.replaceFirst("regex"), "replacement": Replaces first occurance of "regex" with "replacement.
    // s.replaceAll("regex"), "replacement":   Replaces all occurances of "regex" with "replacement.


    public static List removeWhiteSpaceAndQuotes(String stmt) {
        Pattern pattern = Pattern.compile("\\b(?:(?<=\'|\")[^\'|\"]*(?=\')|\\w+)\\b");
        Matcher matcher = pattern.matcher(stmt);
        List<String> st = new ArrayList<>();
        while (matcher.find()) {
            st.add(matcher.group(0).toString());
        }
        return st;
    }

    public static void main(String[] args) {

        String stmt = new String("create \"tab1\", \"fam1\", 'fam2'");

        List<String> rmspaceStmt = removeWhiteSpaceAndQuotes(stmt);
        System.out.println(rmspaceStmt);
        System.out.println(rmspaceStmt.size());
    }
}