package com.ifeng.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhanglr on 2016/3/29.
 */
public class IpsFieldsCombination {
    private List<String> arr = new ArrayList();
    private List<List<String>> colList = new ArrayList<List<String>>();

    public List<List<String>> getCols() {
        arr.add("requestType");
        arr.add("nodeIp");
        arr.add("clientType");
        arr.add("hostIp");
        caculate(0, new ArrayList<String>(), arr);
        return colList;
    }

    private void caculate(int i, List<String> str, List<String> cols) {
        if (i == cols.size()) {
            if (str.size() > 0) {
                colList.add(str);
            }
            return;
        }

        List<String> res = new ArrayList<String>();
        res.addAll(str);
        res.add(cols.get(i));
        caculate(i + 1, res, cols);
        caculate(i + 1, str, cols);
    }
}
