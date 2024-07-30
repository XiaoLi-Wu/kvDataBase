/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import controller.SocketServerHandler;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;
public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";
    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor();//异步操作

    private static final long MAX_FILE_SIZE =  2 * 1024; //文件最大大小
    private AtomicInteger currentFileNumber = new AtomicInteger(0);//最大文件序号
    private List<File> dataFiles = new ArrayList<>();//数据文件列表


    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量,通过键去查找上面的数据
     * */
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
    private final int storeThreshold=8;

    /**
     * 索引标识
     */
    private volatile boolean indexReloaded = false;

    private ArrayList<String> getStrings = new ArrayList<>();

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","目录不存在，正在创建...");
            file.mkdirs();
        }
        File file1 = new File(this.genFilePath());
        if (!file1.exists()) {
            try {
                file1.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("无法创建数据文件", e);
            }
        }
        loadExistingFiles();
        this.reloadIndexAsync();
    }

    //加载已存在的文件
    private void loadExistingFiles() {
        File dir = new File(dataDir);
        File[] files = dir.listFiles((d, name) -> name.startsWith(NAME) && name.endsWith(TABLE));
        if (files != null) {
            Arrays.sort(files);
            dataFiles.addAll(Arrays.asList(files));//转移数据到全局变量
            if (!dataFiles.isEmpty()) {
                String lastName = dataFiles.get(dataFiles.size() - 1).getName();//获取最后一个文件的名称
                currentFileNumber.set(Integer.parseInt(lastName.substring(NAME.length(), lastName.length() - TABLE.length())));//更新最后一个文件
            }
        }
    }

    private String genFilePath() {
        return this.dataDir + File.separator + NAME + currentFileNumber.get() + TABLE;
    }

    //检查该日志文件大小是否超过阈值,超过则创建新的文件并将文件序号加1
//    private void checkAndRotateFile() {
//        File currentFile = new File(genFilePath());
//        System.out.println("currentFile大小："+currentFile.length());
//        if (currentFile.length() >= MAX_FILE_SIZE) {
//            dataFiles.add(createNewDataFile());
//        }
//    }

    //异步加载索引
    public void reloadIndexAsync(){
        compactionExecutor.submit(()->{
            try {
                reloadIndex();
            } finally {
                indexReloaded = true;
                for (String str :
                        getStrings) {
                    System.out.println(str+"的值为："+ get(str));
                }
            }
            return null;
        });
    }
    //刷新索引
    //加载所有索引（实现回放功能）
    public void reloadIndex() {
        for (File file : dataFiles) {
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
                byte[] intBuffer = new byte[4];//读取指令的长度
                byte[] cmdBuffer;//指令内容
                int processed = 0;//读取次数
                long start = 0;//读取位置
                //从文件名中提取文件的编号
                int fileNumber = Integer.parseInt(file.getName().substring(NAME.length(), file.getName().length() - TABLE.length()));//第几个文件

                while (bis.read(intBuffer) != -1) {
                    int cmdLen = ByteBuffer.wrap(intBuffer).getInt();
                    cmdBuffer = new byte[cmdLen];

                    if (bis.read(cmdBuffer, 0, cmdLen) != cmdLen) {
                        break;
                    }

                    JSONObject value = JSON.parseObject(cmdBuffer, 0, cmdLen, StandardCharsets.UTF_8, JSONObject.class);
                    Command command = CommandUtil.jsonToCommand(value);

                    if (command != null) {
                        CommandPos cmdPos = new CommandPos((int) start + 4, cmdLen, fileNumber);
                        index.put(command.getKey(), cmdPos);
                    }

                    start += 4 + cmdLen;
                    processed++;

                    if (processed % 1000 == 0) {
                        index.entrySet().removeIf(entry -> entry.getValue() == null);
                        System.gc();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        LoggerUtil.debug(LOGGER, logFormat, "读取的索引", index.toString());
    }

    //新建键值对指令，先将指令的长度写入文件中，再将指令写到内存表中，最后通过阈值写进文件中
    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
//            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            memTable.put(key,command);
            if (memTable.size() >= storeThreshold && indexReloaded) {
                flushMemTableToDisk();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
            asyncCompact();
//            checkAndRotateFile();
        }
    }

    //获得键的值，传入键，返回字符串的值
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            if(!indexReloaded){
                getStrings.add(key);
                return "索引正在加载中，在加载完毕之后会输出结果";
            }
            String result=getMemTable(key);
            if(result!=null){
                if (result.equals("rm")){
                    return null;
                }
                return result;
            }
            // 从索引中获取信息的位置以及长度
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }
            String filePath = this.dataDir + File.separator + NAME + cmdPos.getFileNumber() + TABLE;
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(filePath, cmdPos.getPos(), cmdPos.getLen());

            JSONObject value = JSONObject.parseObject(new String(commandBytes));//反序列化
            Command cmd = CommandUtil.jsonToCommand(value);//获取指令的类型
            if (cmd instanceof SetCommand) {
                //属于存在类型就输出值
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                //属于rm删除类型则返回空
                return null;
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
        return null;
    }

    public String getMemTable(String key){
        Command cmd = memTable.get(key);
        if (cmd != null) {
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return "rm";
            }
        }
        return null;
    }

    //删除方法，需要传入键，返回为空
    @Override
    public void rm(String key) {
        try {
            RmCommand command = new RmCommand(key);
            //加锁
            indexLock.writeLock().lock();
            memTable.put(key, command);
            if (memTable.size() >= storeThreshold && indexReloaded) {
                flushMemTableToDisk();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
            asyncCompact();//压缩
//            checkAndRotateFile();
        }
    }

    //
    public void flushMemTableToDisk() {
        try {
            for (Map.Entry<String, Command> entry : memTable.entrySet()) {
                byte[] commandBytes = JSONObject.toJSONBytes(entry.getValue());
                RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
                int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
                CommandPos cmdPos = new CommandPos(pos, commandBytes.length, currentFileNumber.get());
                index.put(entry.getKey(), cmdPos);
            }
            memTable.clear();
        } catch (Exception e) {
            throw new RuntimeException("将memTable刷新到磁盘时出错", e);
        }
    }


    //判断文件是否需要压缩
    private boolean needCompaction() {
        File dataFile = new File(this.genFilePath());
        return dataFile.length() >= MAX_FILE_SIZE ; //如果文件大于MAX_FILE_SIZE这个文件最大值的时候，则进行压缩
    }

    //文件压缩执行文件
    public void compact() {
        if (!needCompaction()) {
            LoggerUtil.debug(LOGGER, "不需要压缩，跳过。");
            return;
        }

        indexLock.writeLock().lock();
        try {
            LoggerUtil.info(LOGGER, "开始压缩过程。");
            //接收所有的指令
            Map<String, Command> latestCommands = new ConcurrentHashMap<>();
            List<File> filesToCompress = new ArrayList<>(dataFiles);

            // 使用线程池读取所有命令，并且进行管理
            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            List<Future<Map<String, Command>>> futures = new ArrayList<>();

            // 读取所有命令，按照一定的线程顺序写入列表
            for (File file : filesToCompress) {
                futures.add(executorService.submit(() -> {
                    //使用多线程读取每个文件里面的命令
                    return readCommandsFromFile(file);
                }));
            }
            LoggerUtil.info(LOGGER, "从 {} 个文件中读取了 {} 条指令。", filesToCompress.size(), latestCommands.size());

            // 等待所有文件读取完成
            for (Future<Map<String, Command>> future : futures) {
                try {
                    latestCommands.putAll(future.get());
                } catch (InterruptedException | ExecutionException e) {
                    LoggerUtil.error(LOGGER, e, "文件读取过程中发生错误");
                    throw new RuntimeException("文件读取失败", e);
                }
            }

            executorService.shutdown();//关闭线程池
            LoggerUtil.info(LOGGER, "完成多线程的文件压缩过程");
            // 删除旧文件
            int deletedFiles = 0;
            for (File file : filesToCompress) {
                if (file.delete()) {
                    deletedFiles++;
                } else {
                    LoggerUtil.info(LOGGER, "无法删除文件: {}", file.getName());
                }
            }
            LoggerUtil.info(LOGGER, "删除了 {} 个旧文件。", deletedFiles);

            // 创建新的文件列表
            List<File> newFiles = new ArrayList<>();
            currentFileNumber.set(-1); // 重置文件编号，确保从0开始
            File currentFile = createNewDataFile();
            newFiles.add(currentFile);

            // 重建索引
            index.clear();
            AtomicLong currentFileSize = new AtomicLong(0);
            AtomicInteger commandsWritten = new AtomicInteger(0);

            // 使用线程安全的队列来存储待写入的命令
            BlockingQueue<Command> commandQueue = new LinkedBlockingQueue<>(latestCommands.values());

            // 创建写入线程
            Thread writerThread = new Thread(() -> {
                try {
                    while (true) {
                        Command command = commandQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (command == null && commandQueue.isEmpty()) {
                            break;
                        }
                        if (command != null) {
                            //写入文件！！！！！！********！！！！！！！！******——————————————
                            writeCommand(command, newFiles, currentFileSize, commandsWritten);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LoggerUtil.error(LOGGER, e, "写入线程被中断");
                }
            });

            writerThread.start();

            try {
                writerThread.join();
            } catch (InterruptedException e) {
                LoggerUtil.error(LOGGER, e, "等待写入线程完成时被中断");
                Thread.currentThread().interrupt();
            }

            LoggerUtil.info(LOGGER, "向 {} 个新文件写入了 {} 条指令。", newFiles.size(), commandsWritten);

            //删掉旧的添加新的
            dataFiles.clear();
            dataFiles.addAll(newFiles);

            LoggerUtil.info(LOGGER, "压缩成功完成。新文件数量: {}", newFiles.size());
        } catch (Exception e) {
            LoggerUtil.error(LOGGER, e, "压缩过程中发生错误");
            throw new RuntimeException("压缩失败", e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    //写入指令和文件轮滚
    //将读出的数据写入到新的文件newFile
    private void writeCommand(Command command, List<File> newFiles, AtomicLong currentFileSize, AtomicInteger commandsWritten) {
        byte[] commandBytes = JSONObject.toJSONBytes(command);
        int totalLength = 4 + commandBytes.length;

        synchronized (this) {//自动rotate
            if (currentFileSize.get() + totalLength > MAX_FILE_SIZE) {
                File newFile = createNewDataFile();
                newFiles.add(newFile);
                currentFileSize.set(0);
                LoggerUtil.debug(LOGGER, "创建新文件: {}", newFile.getName());
            }

            File currentFile = newFiles.get(newFiles.size() - 1);
            String currentFilePath = currentFile.getAbsolutePath();

            RandomAccessFileUtil.writeInt(currentFilePath, commandBytes.length);
            int pos = RandomAccessFileUtil.write(currentFilePath, commandBytes);

            CommandPos cmdPos = new CommandPos(pos, commandBytes.length, currentFileNumber.get());
            index.put(command.getKey(), cmdPos);

            currentFileSize.addAndGet(totalLength);
            commandsWritten.incrementAndGet();
        }
    }

    //读取文件信息，将文件中的指令读进Map类变量中，存进去的信息是command指令
    private Map<String, Command> readCommandsFromFile(File file) {
        Map<String, Command> commands=new HashMap<>();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
            byte[] intBuffer = new byte[4]; // 读取指令的长度
            byte[] cmdBuffer; // 指令内容
//            int processed = 0; // 读取次数
            while (bis.read(intBuffer) != -1) {
                int cmdLen = ByteBuffer.wrap(intBuffer).getInt();
                cmdBuffer = new byte[cmdLen];

                if (bis.read(cmdBuffer, 0, cmdLen) != cmdLen) {
                    break;
                }
                try {
                    JSONObject value = JSON.parseObject(cmdBuffer, 0, cmdLen, StandardCharsets.UTF_8, JSONObject.class);
                    Command command = CommandUtil.jsonToCommand(value);
                    commands.put(command.getKey(), command);
                } catch (JSONException e) {
                    LOGGER.error("文件JSON化失败: " + file.getName(), e);
                }
            }
        } catch (IOException e) {
            LOGGER.error("文件读取失败: " + file.getName(), e);
        }
        return commands;
    }

    // 改进的异步压缩方法
    public CompletableFuture<Void> asyncCompact() {
        return CompletableFuture.runAsync(this::compact, compactionExecutor)//执行压缩
                .exceptionally(e -> {
                    LOGGER.error("Async compaction failed", e);
                    return null;
                });
    }


    //    @Override
//    public void close() throws IOException {
//        flushMemTableToDisk();
//        compactionExecutor.shutdown();
//        memTable.clear();
//        index.clear();
//    }
    @Override
    public void close() throws IOException {
        flushMemTableToDisk();
        compactionExecutor.shutdown();
        try {
            if (!compactionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
        }
        memTable.clear();
        index.clear();
    }

    //获取指定文件编号的文件路径
    private String getFilePathByNumber(int fileNumber) {
        return this.dataDir + File.separator + NAME + fileNumber + TABLE;
    }

    //创建新的数据文件,用来新增日志文件
    private File createNewDataFile() {
        int newFileNumber = currentFileNumber.incrementAndGet();
        File newFile = new File(getFilePathByNumber(newFileNumber));
        try {
            newFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException("无法创建新的数据文件", e);
        }
        dataFiles.add(newFile);
        return newFile;
    }
}