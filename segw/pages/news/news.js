// pages/news/news.js

var app=getApp();
var a=false;
var tText=['1','2','3','4'];
var tTitle=['t1','t2','t3','t4'];
var iImgsrc=['','','',''];
var aAudiosrc=['','','',''];
var ImgSrc=['','','',''];
var pageNumber=0;
var viewNumber=[0,1,2,3];
var colnum=0;
var jsonData;
var HideView=['display:block','display:block','display:block','display:block'];
var HideImg=['display:block','display:block','display:block','display:block'];
var hid="display:none";
var non="display:block";
var currentSchool="";
var onPlay=4;
var playIcon ="https://www.vdebug.xyz/img/play.jpg";
var stopIcon ="https://www.vdebug.xyz/img/stop.jpg";

Page({


  //更新新闻内容
  changeText:function(){
    console.log(jsonData);
    var newsNumber=jsonData[0];
    for(var i=0;i<4;++i){
      viewNumber[i]=pageNumber*4+i+1;
      /**
       * 在这里通过某种方式修改新闻内容
       */
      if(viewNumber[i]>newsNumber){
        HideView[i]=hid;
      }
      else{
        HideView[i]=non;
        tTitle[i]=jsonData[viewNumber[i]].title;
        tText[i]=jsonData[viewNumber[i]].abstract;
        iImgsrc[i]=jsonData[viewNumber[i]].imagepath;
        aAudiosrc[i]=jsonData[viewNumber[i]].audiopath;

        console.log(iImgsrc[i]);

      }

      if(iImgsrc[i]=="error"){
        HideImg[i]=hid;
      }
      else{
        HideImg[i]=non;
      }
    }

    this.setData({hideView:HideView});
    this.setData({text:tText});
    this.setData({title:tTitle});
    this.setData({imgsrc:iImgsrc});
    this.setData({audiosrc:aAudiosrc});
    this.setData({hideImg:HideImg});
  },

  playm: function (n) {
    var This=this;
    wx.playBackgroundAudio({
      dataUrl: aAudiosrc[new Number(n)],
      success: function (res) {
        // success
        console.log("2333");
        onPlay = n;
        ImgSrc[n]=stopIcon;
        This.setData({imgSrc:ImgSrc});
      },
      fail: function (res) {
        // fail
      },
      complete: function (res) {
        // complete
      }
    })
  },

  stopm: function () {
    wx.stopBackgroundAudio();
    var i=0;
    for(i=0;i<4;++i){
      ImgSrc[i]=playIcon;
    }
    onPlay = 4;
    this.setData({ imgSrc: ImgSrc});
  },

  playaudio:function(e){
    var ID=e.currentTarget.id;
    console.log(ID);
    console.log(onPlay);
    if(ID==onPlay){
      this.stopm();
    }
    else{
      this.stopm();
      this.playm(ID);
    }

  },

  //两个按钮
  up:function(e){
    --pageNumber;
    if(pageNumber<=0)
      pageNumber=0;
    this.changeText();
    
    this.stopm();
  },
  down:function(e){
    var maxNews=(pageNumber+1)*4;
    if(maxNews<jsonData[0]){
      pageNumber+=1;
      console.log(maxNews);
    }
    this.changeText();
    this.stopm();
  },

  showContext:function(e){
    var This=this;
    var ID=e.currentTarget.id;
    wx.navigateTo({
      url: '../context/context',
      success: function(res){
      },
      fail: function(res) {
        // fail
        console.log(res);
      },
      complete: function(res) {
        app.globalData.newsNumber = new Number(pageNumber * 4) + new Number(ID);
        console.log(ID);
        console.log(app.globalData.newsNumber);
        app.globalData.context = jsonData[app.globalData.newsNumber].context;
        // complete
      }
    })
  },


  data:{
    text:tText,
    title:tTitle,
    imgsrc:iImgsrc,
    audiosrc:aAudiosrc,
    imgSrc:ImgSrc,
    detailText:'233',
    hideView:HideView
  },
  onLoad:function(options){
    // 页面初始化 options为页面跳转所带来的参数
    pageNumber=0;
  },
  onReady:function(){
    // 页面渲染完成

    var This=this;

    currentSchool=app.globalData.school;
    console.log(currentSchool);

    wx.request({
      url: 'https://www.vdebug.xyz/json/'+currentSchool+'.json',
      data: {
        charset:"utf-8"
      },
      method: 'GET', // OPTIONS, GET, HEAD, POST, PUT, DELETE, TRACE, CONNECT
      // header: {}, // 设置请求的 header
      success: function(res){
        jsonData=res.data;
        
        console.log(jsonData)
      },
      fail: function(res) {
        // fail
      },
      complete: function(res) {
        // complete
        This.changeText();
      }
    })

    
  },
  onShow:function(){
    // 页面显示
    this.stopm();
  },
  onHide:function(){
    // 页面隐藏
  },
  onUnload:function(){
    // 页面关闭
  }
})