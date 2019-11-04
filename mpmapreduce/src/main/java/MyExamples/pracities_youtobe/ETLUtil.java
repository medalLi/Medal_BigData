package MyExamples.pracities_youtobe;

/**
 * @author medal
 * @create 2019-03-28 22:33
 **/

/*
RX24KLBhwMI	lemonette	697	People & Blogs	512	24149	4.22	315	474	t60tW0WevkE	WZgoejVDZlo	Xa_op4MhSkg	MwynZ8qTwXA	sfG2rtAkAcg	j72VLPwzd_c	24Qfs69Al3U	EGWutOjVx4M	KVkseZR5coU	R6OaRcsfnY4	dGM3k_4cNhE	ai-cSq6APLQ	73M0y-iD9WE	3uKOSjE79YA	9BBu5N0iFBg	7f9zwx52xgA	ncEV0tSC7xM	H-J8Kbx9o68	s8xf4QX1UvA	2cKd9ERh5-8
MEvoy_owET8	smpfilms	736	Travel & Places	921	109673	4.25	1181	774	YtX2nwowMtU	A5dp02FXDmM	bGoUu4gAHaI	faDB-ToajhM	srcg9xLjtuE	9aE4eMVeUEw	G5fZky7Nm1k	UEXvMJo3ZAY	sCTbH-VP7mA	WiriPTfpIP8	W-s_e61hkys	cQWtiU6d99w	93LHxjgQ4LE	JEiATJFBWO0	JzjnhpqWIPs	G_qfXiOkYPU	Gd6M-B3FOaQ	Y5pMgbhyb18	7jdAdCmMRkg	yxbLFd6Y38E
* */
public class ETLUtil {
    public static void main(String[] args) {
        String line1 = "RX24KLBhwMI\tlemonette\t697\tPeople & Blogs\t512\t24149\t4.22\t315\t474";
        String line2 = "MEvoy_owET8\tsmpfilms\t736\tTravel & Places\t921\t109673\t4.25\t1181\t774\tYtX2nwowMtU\tA5dp02FXDmM\tbGoUu4gAHaI\tfaDB-ToajhM\tsrcg9xLjtuE\t9aE4eMVeUEw\tG5fZky7Nm1k\tUEXvMJo3ZAY\tsCTbH-VP7mA\tWiriPTfpIP8\tW-s_e61hkys\tcQWtiU6d99w\t93LHxjgQ4LE\tJEiATJFBWO0\tJzjnhpqWIPs\tG_qfXiOkYPU\tGd6M-B3FOaQ\tY5pMgbhyb18\t7jdAdCmMRkg\tyxbLFd6Y38E\n";
        System.out.println(method(line1));
        System.out.println(method(line2));
    }

    public static String method(String line){
        String[] lineSplit = line.split("\t");
        lineSplit[3] = lineSplit[3].replace(" ","");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i <lineSplit.length ; i++) {
            sb.append(lineSplit[i]);
            if(i < 9){
                if(i<lineSplit.length-1){
                    sb.append("\t");
                }
            }else{
                if(i<lineSplit.length-1){
                    sb.append("&");
                }
            }
        }
        return new String(sb);
    }
}
