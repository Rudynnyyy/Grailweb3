;(function () {
  function safeCall(fn) {
    try {
      fn()
    } catch {}
  }

  function createInstance(opts) {
    return {
      show: function () {
        safeCall(function () {
          if (opts && typeof opts.success === "function") opts.success("")
        })
      },
      destroy: function () {},
    }
  }

  window.initAliyunCaptcha = function initAliyunCaptcha(opts) {
    safeCall(function () {
      if (opts && typeof opts.getInstance === "function") opts.getInstance(createInstance(opts))
    })
  }
})()
