module BlocRecord
  class Collection < Array

    def update_all(updates)
       ids = self.map(&:id)
       # #6
       self.any? ? self.first.class.update(ids, updates) : false
    end

    def take(num=1)
			self[0..num-1]
		end

		def where(data)
			result = []
			for obj in self
				for key in data.keys
					result += self.first.class.where("id" => obj.id, key => data[key])
				end
			end
			result
		end

		def not(data)
			result = []
			for obj in self
				for key in data.keys
					result += self.first.class.where("id" => obj.id, key => !data[key])
				end
			end
		end

    def destroy_all
			self.each do |obj|
				obj.destroy
			end
			true
		end

  end
end
