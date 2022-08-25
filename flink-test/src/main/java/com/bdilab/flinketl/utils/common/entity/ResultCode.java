package com.bdilab.flinketl.utils.common.entity;

/**
 * 结果码
 * 正常结果为 1 开头
 * 异常结果为 2 开头
 */
public enum ResultCode {

    SUCCESS(true, 10000, "操作成功！"),

    TEST_SUCCESS(true, 10000, "连接成功！"),

    // 下列为常见异常，错误信息可以自定义
    // -------------------------------------------------------
    // 校验失败
    VALIDATED_FAIL(false, 10010, ""),
    // 不存在
    INFO_NOT_EXIST(false, 10012, ""),
    // 非法参数
    ILLEGAL_ARGUMENT(false, 10013, ""),
    // 接口运行错误
    RUNTIME_FAIL(false, 10014, ""),
    // 用户操作错误
    ABNORMAL_OPERATION(false, 10024, "用户操作错误"),
    // -------------------------------------------------------


    // 系统的正常错误
    UNAUTHENTICATED(false, 10002, "您还未登录"),
    UNAUTHORISE(false, 10003, "权限不足"),
    COUNT_BANED(false, 10004, "因多次密码错误，用户已被锁定，锁定时长5分钟"),
    COUNT_NOT_EXITS(false, 10005, "账户不存在"),
    COUNT_NOT_LOGIN(false, 10006, "账户未登录"),
    COUNT_LOGINED(false, 10007, "账户已登录"),
    WRONG_PWD(false, 10008, "账户密码错误"),
    EXPIRATION(false, 10009, "登录已过期"),
    COUNT_CANT_EDIT(false, 10010, "账户不允许修改"),

    // 查询错误
    INFO_NOTIN_DATABASE(false, 10011, "数据库中不存在该信息"),

    // 文件错误
    FILE_CANT_EMPTY(false, 10015, "文件不能为空"),
    FILE_UPLOAD_FAIL(false, 10016, "文件上传失败"),
    FILE_TYPE_WRONG(false, 10017, "文件类型错误"),
    FILE_NOT_EXIST(false, 10018, "文件不存在"),
    FILE_IO_FAIL(false, 10019, "文件读取失败"),
    FILE_CONTENT_EMPTY(false, 10020, "文件内容为空"),
    FILE_SHEET_EMPTY(false, 10021, "表为空"),
    FILE_COLUMNS_EMPTY(false, 10022, "表数据为空"),

    // 用户操作错误
    USER_NO_PERMISSION(false, 10023, "当前用户无此权限"),

    // 邮件发送错误
    SEND_EMAIL_FAIL(false, 10024, "邮件发送失败"),

    // 验证码错误
    GET_CODE_SUSS(false, 10025, "获取验证码成功"),
    GET_CODE_FAIL(false, 10026, "获取验证码失败"),
    INVALID_CODE(false, 10027, "验证码已失效"),
    WRONG_VERIFYCODE(false, 10028, "验证码错误"),

    // 任务提交错误
    TASK_COMMIT_FAIL(false, 10029, "任务提交失败"),

    // API服务错误
    API_ERROR(false, 10030, "数据接口调用失败！请联系管理员开启"),

    // 接口请求超时
    TIME_OUT(false, 10031, "接口请求超时"),

    // 消息服务错误返回码
    MESSAGE_SERVICE_ERRROR_CODE(false, 10040, "操作失败"),
    MESSAGE_SERVICE_OPTION_KAFKA_ERROR(false, 10040, "kafka操作失败"),

    //免登录失败
    LOGIN_FAILED(false, 10041, "登录失败"),


    // 系统错误返回码
    FAIL(false, 20000, "操作失败"),

    // 系统的异常错误
    SERVER_ERROR(false, 20001, "系统错误");

    //操作是否成功
    boolean success;
    //操作代码
    int code;
    //提示信息
    String message;

    ResultCode(boolean success, int code, String message) {
        this.success = success;
        this.code = code;
        this.message = message;
    }

    public boolean success() {
        return success;
    }

    public int code() {
        return code;
    }

    public String message() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
