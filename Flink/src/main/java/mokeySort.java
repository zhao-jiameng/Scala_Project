import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @PROJECT_NAME: Flink
 * @PACKAGE_NAME: PACKAGE_NAME
 * @author: 赵嘉盟-HONOR
 * @data: 2022-10-27 16:49
 * @DESCRIPTION
 */
public class mokeySort {
    public static void main(String[] args) {
        //初始化一个集合
        List<Integer> list = new ArrayList<>();
        Collections.addAll(list,23,4,56,45,12,33,76,84,66,96);
        MonkeySort(list);
    }
    private static void MonkeySort(List<Integer> list){
        int sum = 0;
        //开始时间
        long start = System.currentTimeMillis();
        while (true){
            ++sum;
            System.out.println("正在进行第"+sum+"排序");
            System.out.println("排序之前"+list.toString());
            //把集合进行重新排列
            Collections.shuffle(list);
            if (isSort(list)){
                //结束时间
                long end = System.currentTimeMillis();
                System.out.println("排列完成");
                System.out.println("排序之后"+list.toString());
                System.out.println("用时"+(end-start)/1000+"秒");
                return;
            }
        }
    }

    private static boolean isSort(List<Integer> list) {
        for (int i = 0; i < list.size()-1; i++) {
            if (list.get(i)>list.get(i+1)){
                return false;
            }
        }
        return true;
    }
}
