// pages/context/context.js

var app=getApp();
var hid="display:none";
var sho="display:block";
var HideText = [sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho, sho];
var Par = ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''];


Page({
  data:{
    hideText:HideText,
    par:Par
  },
  onLoad:function(options){
    // 页面初始化 options为页面跳转所带来的参数
  },
  onReady:function(){

    var context=app.globalData.context;
    var len=context[0];
    var con=context[1];
    var i=1;
    for(i=0;i<26;++i){
      HideText[i]=hid;
    }
    var j=0;
    for(i=0;i<len;++i){
      if (con[i].length<4){
        continue;
      }
      else{
        HideText[j] = sho;
        Par[j] = "　　" + con[i];
        ++j;
      }
    }
    for(j;j<26;++j){
      HideText[j]=hid;
    }

    this.setData({hideText:HideText});
    this.setData({par:Par});
    
  },
  onShow:function(){
    // 页面显示
  },
  onHide:function(){
    // 页面隐藏
  },
  onUnload:function(){
    // 页面关闭
  }
})