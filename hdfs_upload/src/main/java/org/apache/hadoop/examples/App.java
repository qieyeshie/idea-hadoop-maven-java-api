package org.apache.hadoop.examples;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



//HDFS Upload

/*
public class App{
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.137.141:9000");
        //指定用户名，获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"zkpk");

        //Local file
        Path src=new Path("D:\\test.txt");
        //HDFS file
        Path dst=new Path("/mydir/test.txt");
        fs.copyFromLocalFile(src,dst);

        //不需要再操作FileSystem，关闭
        fs.close();

        System.out.println("Upload Successfully!");
    }
}

*/


//将本地D:\test.txt文件上传至HDFS的/mydir下

/*
public class App{
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.137.141:9000");
        //指定用户名，获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"zkpk");

        //构造一个输入流
        InputStream is=new FileInputStream("D:\\test.txt");

        //得到一个输入流
        OutputStream os=fs.create(new Path("/mydir/test2.txt"));

        //使用工具类实现复制
        IOUtils.copyBytes(is,os,10241);

        //关闭流
        is.close();
        os.close();

        //不需要再操作FileSystem，关闭
        fs.close();

        System.out.println("Upload Successfully!");
    }
}
*/


//在HDFS上创建/mydir/test3.txt文件

/*
public class App{
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.137.141:9000");
        //指定用户名，获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"zkpk");

        //define new file
        Path dfs=new Path("/mydir/test3.txt");
        FSDataOutputStream os=fs.create(dfs,true);
        os.writeBytes("hello,hdfs!");

        //关闭流
        os.close();

        //不需要再操作FileSystem，关闭
        fs.close();

        System.out.println("Create Successfully!");
    }
}
*/


//查看HDFS上的/mydir/test.txt文件的详细信息

/*
public class App{
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.137.141:9000");
        //指定用户名，获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"zkpk");

        //指定路径
        Path path=new Path("/mydir/test.txt");

        //获取状态
        FileStatus fileStatus=fs.getFileLinkStatus(path);

        //获取数据块大小
        long blockSize=fileStatus.getBlockSize();
        System.out.println("blockSize:"+blockSize);

        //获取文件大小
        long fileSize=fileStatus.getLen();
        System.out.println("fileSize:"+fileSize);

        //获取文件拥有者
        String fileOwner=fileStatus.getOwner();
        System.out.println("fileOwner:"+fileOwner);

        //获取最近访问时间
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
        long accessTime=fileStatus.getAccessTime();
        System.out.println("accessTime:"+sdf.format(new Date(accessTime)));

        //获取最后修改时间
        long modifyTime=fileStatus.getModificationTime();
        System.out.println("modifyTime:"+sdf.format(new Date(modifyTime)));

        //不需要再操作FileSystem，关闭
        fs.close();

        System.out.println("Detail Successfully!");
    }
}
*/


//将HDFS上的/mydir/test.txt文件下在到本地D:\\test2.txt

/*
public class App{
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.137.141:9000");
        //指定用户名，获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"zkpk");

        //HDFS file
        Path src=new Path("/mydir/test.txt");
        //local file
        Path dst=new Path("D:\\test2.txt");

        //Linux下
        //fs.copyToLocalFile(src,dst);

        //Windows下
        fs.copyToLocalFile(false,src,dst,true);

        //不需要再操作FileSystem，关闭
        fs.close();

        System.out.println("Dowload Successfully!");
    }
}
*/


/*

public class App{
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.137.141:9000");
        //指定用户名，获取FileSystem对象
        FileSystem client=FileSystem.get(uri,conf,"zkpk");

        //打开一个输入流<<HDFS
        InputStream is = client.open(new Path("/mydir/test.txt"));

        //构造一个输出流>>D:\test3.txt
        OutputStream os = new FileOutputStream("D:\\test3.txt");

        //使用工具类实现渎职
        IOUtils.copyBytes(is,os,1024);

        //关闭流
        is.close();
        os.close();

        //不需要再操作FileSystem，关闭
        client.close();

        System.out.println("Download Successfully!");
    }
}
*/



//删除HDFS上的/mydir/test2.txt文件

public class App{
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        //配置NameNode地址
        URI uri=new URI("hdfs://192.168.137.141:9000");
        //指定用户名，获取FileSystem对象
        FileSystem fs=FileSystem.get(uri,conf,"zkpk");

        //HDFS file
        Path path=new Path("/mydir/test2.txt");
        fs.delete(path,true);

        //不需要再操作FileSystem，关闭
        fs.close();

        System.out.println("Delete Successfully!");
    }
}

